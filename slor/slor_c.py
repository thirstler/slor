import sys
from multiprocessing.connection import Client
import random
import time
from shared import *
from stat_handler import statHandler
from db_ops import SlorDB
import json
import copy

class SlorControl:
    """
    Single instance class containing all of the routines for creating workloads
    from command-line input, sending them to workers and handling simple
    console output.
    """
    config = None
    conn = None
    readmap = None
    stat_buf = None
    last_stage = None
    slordb = None
    mixed_count = None
    stage_itr = None

    def __init__(self, root_config):
        self.config = root_config
        self.slordb = SlorDB(root_config)
        self.mixed_count = 0
        self.conn = []
        self.readmap = []
        self.stat_buf = []
        self.stage_itr = {}


    def exec(self):

        print(BANNER)

        # Print config
        box_text(self.config_text())

        # Gather and show driver info
        print("")
        if not self.connect_to_driver():
            self.print_message("driver(s) failed check(s), I'm out")
            sys.exit(1)

        print("")
        top_box()
        sys.stdout.write("\u2502 STAGES ({}); ".format(len(self.config["tasks"]["loadorder"])))
        sys.stdout.write("\n\u2502\n")
        for stage_val in self.config["tasks"]["loadorder"]:
            stage, duration = self.parse_stage_string(stage_val)
            if sum(opclass_from_label(stage_val) == opclass_from_label(x) for x in self.config["tasks"]["loadorder"]) > 1:
                self.stage_itr[opclass_from_label(stage)] = -1

        for c, stage in enumerate(self.config["tasks"]["loadorder"]):
            stage, duration = self.parse_stage_string(stage)

            if stage in ("read", "delete", "head", "mixed", "cleanup", "tag"):
                sys.stdout.write("\r\u2502   [   shuffling readmap...   ]   ")
                sys.stdout.flush()
                random.shuffle(self.readmap)

            elif stage == "sleep":
                sys.stdout.write("\u2502     [     sleeping ({})...     ]   ".format(duration))
                sys.stdout.flush()
                time.sleep(duration)
                continue
            elif stage == "readmap":
                self.mk_read_map()
                continue

            self.exec_stage(stage, duration)

        for c in self.conn:
            c.close()

        bottom_box()
        print("\ndone.\n")
        print("run {} analysis --input {}\n".format(sys.argv[0], self.slordb.db_file))


    def config_text(self):

        cft_text = "CONFIGURATION:\n"
        cft_text += "\n"
        cft_text += "Workload name:      {0}\n".format(self.config["name"])
        cft_text += "User key:           {0}\n".format(self.config["access_key"])
        cft_text += "Target endpoint:    {0}\n".format(self.config["endpoint"])
        cft_text += "Stage run time:     {0}s\n".format(self.config["run_time"])
        if self.config["save_readmap"]:
            cft_text += "Saving readmap:     {0}\n".format(self.config["save_readmap"])
        if self.config["use_readmap"]:
            cft_text += "Using readmap:      {0}\n".format(self.config["use_readmap"])
        else:

            cft_text += "Object size(s):     {0}\n".format(
                human_readable(self.config["sz_range"][0], precision=0)
                if self.config["sz_range"][0] == self.config["sz_range"][1]
                else "low: {0}, high: {1} (avg: {2})".format(
                    human_readable(self.config["sz_range"][0], precision=0),
                    human_readable(self.config["sz_range"][1], precision=0),
                    human_readable(self.config["sz_range"][2], precision=0)
                ))
            cft_text += "Key length(s):      {0}\n".format(
                self.config["key_sz"][0]
                if self.config["key_sz"][0] == self.config["key_sz"][1]
                else "low: {0}, high:  {1} (avg: {2})".format(
                    self.config["key_sz"][0],
                    self.config["key_sz"][1],
                    self.config["key_sz"][2]
                ))
            cft_text += "Prepared objects:   {0} (readmap length)\n".format(
                human_readable(self.get_readmap_len(), print_units="ops"))
            cft_text += "Upper IO limit:     {0}\n".format(self.config["iop_limit"])
            cft_text += "Bucket prefix:      {0}\n".format(self.config["bucket_prefix"])
            cft_text += "Num buckets:        {0}\n".format(self.config["bucket_count"])
            cft_text += "Prepared data size: {0}\n".format(
                human_readable(self.config["ttl_prepare_sz"]),
                ((int(self.config["ttl_prepare_sz"])/DEFAULT_CACHE_OVERRUN_OBJ)+1),
                human_readable(DEFAULT_CACHE_OVERRUN_OBJ)
            )

        cft_text += "Driver processes:   {0}\n".format(len(self.config["driver_list"]))
        cft_text += "Procs per driver:   {0} ({1} worker processes total)\n".format(
                self.config["driver_proc"],
                (int(self.config["driver_proc"]) * len(self.config["driver_list"])),
            )
        cft_text += "Cache overrun size: {0} ({1} x {2} objects)\n".format(
                human_readable(self.config["ttl_sz_cache"]),
                (int(self.config["ttl_sz_cache"]/DEFAULT_CACHE_OVERRUN_OBJ)+1),
                human_readable(DEFAULT_CACHE_OVERRUN_OBJ)
            )
        cft_text += "Stats database:     {0}\n".format(self.slordb.db_file)
        cft_text += "Stages:\n"
        stagecount = 0
        mixed_count = 0

        for i, stage in enumerate(self.config["tasks"]["loadorder"]):
            stage = stage[:stage.find(":")] if ":" in stage else stage # Strip label information
            stagecount += 1
            duration = self.config["sleeptime"] if stage == "sleep" else self.config["run_time"]
            print(stage)
            if stage == "mixed":
                mixed_prof = self.config["tasks"]["mixed_profiles"][mixed_count]
                mixed_count += 1
                cft_text += "                    {0}: {1} - ".format(stagecount, stage)
                for j, m in enumerate(mixed_prof):
                    cft_text += "{0}:{1}%".format(m, mixed_prof[m])
                    if (j + 1) < len(mixed_prof):
                        cft_text += ", "
                cft_text += " ({} seconds)\n".format(duration)
            elif stage == "readmap":
                cft_text += "{} {}: {}\n".format(" "*19, stagecount, "readmap - generate keys for use during read operations")
            elif stage == "init":
                cft_text += "{} {}: {}\n".format(" "*19, stagecount, "init - create needed buckets/config")
            elif stage == "prepare":
                cft_text += "{} {}: {}\n".format(" "*19, stagecount, "prepare - write objects needed for read operations")
            elif stage == "blowout":
                cft_text += "{} {}: {}\n".format(" "*19, stagecount, "blowout - overrun page cache")
            elif stage == "read":
                cft_text += "{} {}: {}\n".format(" "*19, stagecount, "read - pure GET workload ({} seconds)".format(duration))
            elif stage == "write":
                cft_text += "{} {}: {}\n".format(" "*19, stagecount, "write - pure PUT workload ({} seconds)".format(duration))
            elif stage == "head":
                cft_text += "{} {}: {}\n".format(" "*19, stagecount, "head - pure HEAD workload ({} seconds)".format(duration))
            elif stage == "delete":
                cft_text += "{} {}: {}\n".format(" "*19, stagecount, "delete - pure DELETE workload ({} seconds)".format(duration))
            elif stage == "tag":
                cft_text += "{} {}: {}\n".format(" "*19, stagecount, "tag - pure tagging (metadata) workload ({} seconds)".format(duration))
            elif stage == "cleanup":
                cft_text += "{} {}: {}\n".format(" "*19, stagecount, "cleanup - remove all objects and buckets")
            elif stage[:5] == "sleep":
                cft_text += "{} {}: {}\n".format(" "*19, stagecount, "sleep for {} seconds".format(duration))

        return cft_text


    def connect_to_driver(self):
        ret_val = True
        self.config["driver_node_names"] = []

        top_box()
        print(u"\u2502 DRIVERS ({})".format(len(self.config["driver_list"])))
        print(u"\u2502")
        for i, hostport in enumerate(self.config["driver_list"]):
            sys.stdout.write(
                u"\u2502 {0}) addr: {1}:{2};".format(i+1, hostport["host"], hostport["port"])
            )
            try:
                self.conn.append(Client((hostport["host"], hostport["port"])))
                self.conn[-1].send({"command": "sysinfo"})
                resp = self.conn[-1].recv()
                self.check_driver_info(resp)
                self.slordb.mk_data_store(hostport["host"])

            except Exception as e:
                sys.stderr.write(
                    "  unexpected exception ({2}) connecting to {0}:{1}, exiting\n".format(
                        hostport["host"], hostport["port"], e
                    )
                )
                ret_val = False
        bottom_box()
        return ret_val

    def check_driver_info(self, data):
        self.config["driver_node_names"].append(data["uname"].node)

        sys.stdout.write(" hostname: {0};".format(data["uname"].node))
        sys.stdout.write(" OS: {0}".format(data["uname"].system))
        if data["slor_version"] != SLOR_VERSION:
            sys.stdout.write("\n")
            print(
                u"\u2502    {}warning{}} version mismatch; controller is {} driver is {}".format(
                    bcolors.WARNING, bcolors.ENDC,
                    SLOR_VERSION, data["slor_version"]
                )
            )

        if data["sysload"][0] >= 1:
            sys.stdout.write("\n")
            sys.stdout.write(
                u"\u2502    {}warning{} driver 1m sysload is > 1 ({:.2f})".format(
                    bcolors.WARNING, bcolors.ENDC,
                    data["sysload"][0]
                )
            )

        for iface in data["net"]:
            if (
                data["net"][iface].errin
                + data["net"][iface].errout
                + data["net"][iface].dropin
                + data["net"][iface].dropout
            ) > 0:
                sys.stdout.write("\n")
                sys.stdout.write(
                    u"\u2502    {}warning{} iface {} has dropped packets or errors".format(
                        bcolors.WARNING, bcolors.ENDC,
                        iface
                    )
                )
        sys.stdout.write("\n")

    def poll_for_response(self, all=True):
        return_msg = []

        for i in range(0, len(self.config["driver_list"])):
            while self.conn[i].poll():
                return_msg.append(self.conn[i].recv())
        return return_msg


    def exec_stage(self, stage, duration):
        """
        Contains the main loop communicating with driver processes
        """

        workloads = []
        n_wrkrs = len(self.config["driver_list"])

        if opclass_from_label(stage) in self.stage_itr:
            self.stage_itr[opclass_from_label(stage)] += 1

        # Create the workloads
        for wc, target in enumerate(self.config["driver_list"]):
            stage_cfg = self.mk_stage(target, stage, wc, duration)
            if stage_cfg:
                workloads.append(stage_cfg)
                self.slordb.commit_stage_config(stage, stage_cfg)
            else:
                return

        # Distribute buckets to driver for init
        if stage == "init":
            for bc in range(0, self.config["bucket_count"]):
                workloads[bc % n_wrkrs]["bucket_list"].append(
                    "{0}{1}".format(self.config["bucket_prefix"], bc)
                )

        # Send workloads to driver
        for i, wl in enumerate(workloads):
            self.conn[i].send({"command": "workload", "config": wl})

        self.block_until_ready()
        resp = self.poll_for_response()

        self.print_message("running stage ({0})".format(stage), verbose=True)

        stats_config = copy.deepcopy(self.config)
        stats_config["mixed_profile"] = self.config["mixed_profiles"][self.mixed_count]

        self.stats_h = statHandler(stats_config, opclass_from_label(stage), duration)
        if self.get_readmap_len() > 0 or stage == "blowout":
            if stage == "blowout":
                self.stats_h.set_count_target(self.config["ttl_sz_cache"]/DEFAULT_CACHE_OVERRUN_OBJ)
            else:
                self.stats_h.set_count_target(self.get_readmap_len())

        # Disply headers for stats output
        self.stats_h.headers(self.mixed_count)

        """
        Primary Loop - monitors the drivers by polling messages from them
        """
        donestack = len(workloads)
        while True:
            group_time = time.time()
            sys.stdout.write("\r")
            for i, wl in enumerate(workloads):
                while self.conn[i].poll():
                    try:
                        mesg = self.conn[i].recv()
                        if self.check_status(mesg) == "done":
                            donestack -= 1
                        self.process_message(mesg)    # Decide what to do with the mssage
                    except EOFError:
                        pass
            if donestack == 0:
                time.sleep(1)
                self.stats_h.show(final=True)
                self.print_message(
                    "threads complete for this stage ({0})".format(stage), verbose=True
                )
                break
            self.stats_h.show()
            time.sleep(0.01)

        if opclass_from_label(stage) == "mixed" and self.mixed_count < len(self.config["mixed_profiles"])-1:
            self.mixed_count += 1

        if stage != "sleep":
            self.last_stage = stage

        del self.stats_h

    def block_until_ready(self):

        sys.stdout.write("\r\u2502 [waiting on worker processes...]")
        sys.stdout.flush()
        global_ready = True

        for i in range(0, len(self.config["driver_list"])):
            ready = self.conn[i].recv()
            if ready["ready"] != True:
                global_ready = False

        if global_ready:
            for i in range(0, len(self.config["driver_list"])):
                self.conn[i].send({"exec": True})

        sys.stdout.write("\r \u2502                                  ")
        sys.stdout.flush()

    def get_readmap_len(self):
        try:
            blksz = (self.config["driver_proc"] * len(self.config["driver_list"]))
            objcount = int(self.config["ttl_prepare_sz"] / self.config["sz_range"][2]) + 1
        except:
            objcount = 0

        return (objcount - (objcount % blksz) + blksz)


    def print_message(self, message, verbose=False):
        if verbose == True and self.config["verbose"] != True:
            return
        print(str(message))


    def process_message(self, message):
        """
        Filters messages from drivers and take the appropriate action
        """
        if type(message) == str:
            print(message)
        elif "message" in message:
            print("{0}: {1}".format(message["w_id"], message["message"]))
        elif "type" in message and message["type"] == "stat":
            self.stats_h.update_standing_sample(message)
            loc_stage_iter = None
            
            self.slordb.store_stat(message,
                    stage_itr=str(self.stage_itr[message["stage"]])
                        if message["stage"] in self.stage_itr
                        else None)
        else:
            pass

    def check_status(self, mesg):
        if "status" in mesg and mesg["status"] == "done":
            return "done"

        return False


    def mk_read_map(self):

        # config items that need to be saved/restored with the readmap
        cfg_keys = ("sz_range", "bucket_prefix", "bucket_count", "key_sz", "ttl_prepare_sz", "prepare_objects")
        cfg_out = ""
        if self.config["use_readmap"]:
            with open(self.config["use_readmap"], "r") as fh:
                rmpcfg = json.load(fh)
                fh.close()
                self.readmap = rmpcfg["readmap"]
                cfg_out += "\u2502     {}: {}\n".format("length", human_readable(len(self.readmap), print_units="ops"))
                for cfg in cfg_keys:
                    try:
                        self.config[cfg] = rmpcfg[cfg]
                        cfg_out += "\u2502     {}: {}\n".format(cfg, self.config[cfg])
                    except:
                        sys.stderr.write("failed to find config item {} in {}\n".format(cfg, self.config["use_readmap"]))
            sys.stdout.write("\r\u2502" + " readmap restored from file:\n")
            sys.stdout.write(cfg_out)
        else:
            sys.stdout.write("C" +  "{}{:<15}".format(" "*26, "throughput"))
            objcount = self.get_readmap_len()
            stat_h = statHandler(self.config, "readmap", 0)
            fin = False
            for z in range(0, objcount):

                self.readmap.append(
                    (
                        "{0}{1}".format(
                            self.config["bucket_prefix"],
                            random.randrange(0, self.config["bucket_count"]),
                        ),
                        self.config["key_prefix"] + gen_key(
                            key_desc=self.config["key_sz"],
                            inc=z,
                            prefix=DEFAULT_READMAP_PREFIX
                        ),
                    )
                )
                if (z+1) == objcount:
                    fin = True
                stat_h.readmap_progress(z, objcount, final=fin)

        if self.config["save_readmap"]:
            save_contents = {"readmap": self.readmap}
            for cfg in cfg_keys:
                save_contents[cfg] = self.config[cfg]

            with open(self.config["save_readmap"], "w") as fh:
                fh.write(json.dumps(save_contents))
                fh.close()

    def parse_stage_string(self, stage_str):
        items = stage_str.split("~")
        if len(items) == 2:
            return items[0], int(items[1])
        else:
            # Sleep time has it's own global config so deal with it here
            return items[0], int(self.config["sleeptime"]) if items[0] == "sleep" else int(self.config["run_time"])

    def mk_stage(self, target, stage, wid, duration):
        
        stage = opclass_from_label(stage)

        # Base config for every stage
        config = {
            "host": target["host"],
            "port": target["port"],
            "w_id": wid,
            "threads": self.config["driver_proc"],
            "access_key": self.config["access_key"],
            "secret_key": self.config["secret_key"],
            "endpoint": self.config["endpoint"],
            "verify": self.config["verify"],
            "region": self.config["region"],
            "run_time": duration + (DRIVER_REPORT_TIMER*2),
            "bucket_count": int(self.config["bucket_count"]),
            "bucket_prefix": self.config["bucket_prefix"],
            "sz_range": self.config["sz_range"],
            "key_sz": self.config["key_sz"],
            "driver_list": self.config["driver_list"],
            "prepare_sz": int(
                self.config["ttl_prepare_sz"] / len(self.config["driver_list"])
            ),
            "cache_overrun_sz": int(
                self.config["ttl_sz_cache"] / len(self.config["driver_list"])
            ),
            "mixed_profile": self.config["mixed_profiles"][self.mixed_count],
            "startup_delay": (DRIVER_REPORT_TIMER/len(self.config["driver_list"])),
            "key_prefix": self.config["key_prefix"]
        }

        # Work out the readmap slices
        chunk = int(len(self.readmap) / len(self.config["driver_list"]))
        offset = chunk * wid
        end_slice = offset + chunk
        mapslice = self.readmap[offset:end_slice]

        if stage == "init":
            config["type"] = "init"
            config["bucket_list"] = []
        if stage == "prepare":
            config["type"] = "prepare"
            config["readmap"] = mapslice
        if stage == "blowout":
            config["type"] = "blowout"
        if stage == "read":
            config["type"] = "read"
            config["readmap"] = mapslice
        if stage == "write":
            config["type"] = "write"
        if stage == "mixed":
            config["type"] = "mixed"
            config["readmap"] = mapslice
        if stage == "delete":
            config["type"] = "delete"
            config["readmap"] = mapslice
        if stage == "head":
            config["type"] = "head"
            config["readmap"] = mapslice
        if stage == "cleanup":
            config["type"] = "cleanup"

        return config
