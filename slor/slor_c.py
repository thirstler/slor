import sys
from multiprocessing.connection import Client
import random
import time
from shared import *
import os.path
import stat_handler
from db_ops import SlorDB

class SlorControl:

    config = None
    conn = []
    readmap = []
    stat_buf = []
    last_stage = None
    slordb = None

    def __init__(self, root_config):
        self.config = root_config
        self.slordb = SlorDB(root_config)

    def exec(self):

        print("\nS3 Load Ruler (SLOR) \u250C"+("\u252C"*11) + "\u2510")
        # Print config
        self.box_text(self.config_text())

        # Gather and show driver info
        print("")
        if not self.connect_to_driver():
            self.print_message("driver(s) failed check(s), I'm out")
            sys.exit(1)
        
        print("")
        self.top_box()
        sys.stdout.write("\u2502 STAGES ({})\n\u2502\n".format(len(self.config["tasks"]["loadorder"])))
        for c, stage in enumerate(self.config["tasks"]["loadorder"]):

            if stage in ("read", "delete", "head", "mixed", "cleanup", "tag"):
                sys.stdout.write("\r\u2502   [shuffling readmap...]   ")
                random.shuffle(self.readmap)

            if stage == "blowout" and self.config["ttl_sz_cache"] == 0:
                continue
            elif stage[:5] == "sleep":
                try:
                    st = int(stage[5:])
                except:
                    st = self.config["sleeptime"]
                sys.stdout.write("\u2502 sleeping ({})...".format(st))
                sys.stdout.flush()
                time.sleep(st)
                continue
            elif stage == "readmap":
                sys.stdout.write("\r\u2502"+" "*26)
                sys.stdout.write("{:<15}".format("throughput"))
                sys.stdout.write("\n")
                self.mk_read_map()
                continue

            self.exec_stage(stage)

        for c in self.conn:
            c.close()

        self.bottom_box()
        print("\ndone.\n")

    def top_box(self):
        print(u"\u250C{0}".format(u"\u2500"*(os.get_terminal_size().columns-1)))

    def bottom_box(self):
        print(u"\u2514{0}".format(u"\u2500"*(os.get_terminal_size().columns-1)))

    def box_text(self, text):
        text_lines = text.split("\n")
        self.top_box()
        for line in text_lines:
            print(u"\u2502 "+line)
        self.bottom_box()


    def config_text(self):

        cft_text = "CONFIGURATION:\n"
        cft_text += "\n"
        cft_text += "Workload name:      {0}\n".format(self.config["name"])
        cft_text += "User key:           {0}\n".format(self.config["access_key"])
        cft_text += "Target endpoint:    {0}\n".format(self.config["endpoint"])
        cft_text += "Stage run time:     {0}s\n".format(self.config["run_time"])
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
        cft_text += "Driver processes:   {0}\n".format(len(self.config["driver_list"]))
        cft_text += "Procs per driver:   {0} ({1} worker processes total)\n".format(
                self.config["driver_proc"],
                (int(self.config["driver_proc"]) * len(self.config["driver_list"])),
            )
        cft_text += "Prepared data size: {0}\n".format(
                human_readable(self.config["ttl_prepare_sz"]),
                ((int(self.config["ttl_prepare_sz"])/DEFAULT_CACHE_OVERRUN_OBJ)+1),
                human_readable(DEFAULT_CACHE_OVERRUN_OBJ)
            )
        cft_text += "Cache overrun size: {0} ({1} x {2} objects)\n".format(
                human_readable(self.config["ttl_sz_cache"]),
                (int(self.config["ttl_sz_cache"]/DEFAULT_CACHE_OVERRUN_OBJ)+1),
                human_readable(DEFAULT_CACHE_OVERRUN_OBJ)
            )
        cft_text += "Stats database:     {0}\n".format(self.slordb.db_file)
        cft_text += "Stages:\n"
        stagecount = 0
        
        for i, stage in enumerate(self.config["tasks"]["loadorder"]):
            stagecount += 1
            if stage == "mixed":
                cft_text += "                    {0}: {1} (".format(stagecount, stage)
                for j, m in enumerate(self.config["tasks"]["mixed_profile"]):
                    cft_text += "{0}:{1}%".format(m, self.config["tasks"]["mixed_profile"][m])
                    if (j + 1) < len(self.config["tasks"]["mixed_profile"]):
                        cft_text += ", "
                cft_text += ")\n"
            elif stage == "readmap":
                cft_text += "{} {}: {}\n".format(" "*19, stagecount, "readmap - generate keys for use during read operations")
            elif stage == "init":
                cft_text += "{} {}: {}\n".format(" "*19, stagecount, "init - create needed buckets/config")
            elif stage == "prepare":
                cft_text += "{} {}: {}\n".format(" "*19, stagecount, "prepare - write objects needed for read operations")
            elif stage == "blowout":
                cft_text += "{} {}: {}\n".format(" "*19, stagecount, "blowout - overrun page cache")
            elif stage == "read":
                cft_text += "{} {}: {}\n".format(" "*19, stagecount, "read - pure GET workload")
            elif stage == "write":
                cft_text += "{} {}: {}\n".format(" "*19, stagecount, "write - pure PUT workload")
            elif stage == "head":
                cft_text += "{} {}: {}\n".format(" "*19, stagecount, "head - pure HEAD workload")
            elif stage == "delete":
                cft_text += "{} {}: {}\n".format(" "*19, stagecount, "delete - pure DELETE workload")
            elif stage == "tag":
                cft_text += "{} {}: {}\n".format(" "*19, stagecount, "tag - pure tagging (metadata) workload")
            elif stage == "sleep":
                cft_text += "{} {}: {}\n".format(" "*19, stagecount, "sleep - delay between workloads")
            elif stage == "cleanup":
                cft_text += "{} {}: {}\n".format(" "*19, stagecount, "cleanup - remove all objects and buckets")

        return cft_text
        

    def connect_to_driver(self):
        ret_val = True
        self.config["driver_node_names"] = []
        
        self.top_box()
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
        self.bottom_box()
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

    def exec_stage(self, stage):
        """
        Contains the main loop communicating with driver processes
        """

        workloads = []
        n_wrkrs = len(self.config["driver_list"])

        # Create the workloads
        for wc, target in enumerate(self.config["driver_list"]):
            stage_cfg = self.mk_stage(target, stage, wc)
            if stage_cfg:
                workloads.append(stage_cfg)
            else:
                return

        # Distribute buckets to driver for init
        if stage == "init":
            for bc in range(0, self.config["bucket_count"]):
                workloads[bc % n_wrkrs]["bucket_list"].append(
                    "{0}{1}".format(self.config["bucket_prefix"], bc)
                )
        
        if stage == "mixed":
            sys.stdout.write("\r")
            sys.stdout.write("\u2502"+" "*26)
            items = []
            for o in self.config["mixed_profile"]:
                items.append("{}".format(o))
            items.append("elapsed")
            for i in items:
                sys.stdout.write("{:<15}".format(i))
            sys.stdout.write("\n")
        elif any(stage == x for x in MIXED_LOAD_TYPES + ("prepare","blowout", "cleanup")) and \
             all(self.last_stage != x for x in MIXED_LOAD_TYPES + ("prepare","blowout", "cleanup")):
            sys.stdout.write("\r")
            sys.stdout.write("\u2502"+" "*26)
            items = []
            for o in ("throughput", "bandwidth", "resp ms", "failures", "elapsed"):
                items.append("{}".format(o))
            for i in items:
                sys.stdout.write("{:<15}".format(i))
            sys.stdout.write("\n")

        # Send workloads to driver
        for i, wl in enumerate(workloads):
            self.conn[i].send({"command": "workload", "config": wl})

        self.print_message("running stage ({0})".format(stage), verbose=True)
        
        self.stats_h = stat_handler.statHandler(self.config, stage)
        if self.get_readmap_len() > 0:
            self.stats_h.set_count_target(self.get_readmap_len())

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

        self.last_stage = stage


    def get_readmap_len(self):
        try:
            blksz = (self.config["driver_proc"] * len(self.config["driver_list"]))
            objcount = int(self.config["ttl_prepare_sz"] / self.config["sz_range"][2]) + 1
        except:
            objcount = 0

        return (objcount - (objcount % blksz) + blksz)


    def mk_read_map(self, key_desc={"min": 40, "max": 40}):
        
        objcount = self.get_readmap_len()
        stat_h = stat_handler.statHandler(self.config, "readmap")
        fin = False
        for z in range(0, objcount):

            self.readmap.append(
                (
                    "{0}{1}".format(
                        self.config["bucket_prefix"],
                        random.randrange(0, self.config["bucket_count"]),
                    ),
                    gen_key(
                        key_desc=self.config["key_sz"], prefix=DEFAULT_READMAP_PREFIX
                    ),
                )
            )
            if (z+1) == objcount:
                fin = True
            stat_h.readmap_progress(z, objcount, final=fin)


    def print_message(self, message, verbose=False):
        if verbose == True and self.config["verbose"] != True:
            return
        print(str(message))


    def process_message(self, message):
        """
        Filters messages from drivers and take the appropriate action
        """
        #try:
        if type(message) == str:
            print(message)
        elif "message" in message:
            print("{0}: {1}".format(message["w_id"], message["message"]))
        elif "type" in message and message["type"] == "stat":
            self.stats_h.update_standing_sample(message)
            self.slordb.store_stat(message)
        else:
            pass  # ignore
        #except Exception as e:
        #    print("fuuuuck: {} - {}".format(str(e), message))

    def check_status(self, mesg):
        if "status" in mesg and mesg["status"] == "done":
            return "done"

        return False
    

    def mk_stage(self, target, stage, wid):

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
            "run_time": self.config["run_time"],
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
            "mixed_profile": self.config["mixed_profile"],
            "startup_delay": (DRIVER_REPORT_TIMER/len(self.config["driver_list"]))
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
