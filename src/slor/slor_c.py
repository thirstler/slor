from slor.shared import *
from slor.stat_handler import statHandler
from slor.slor_db import _slor_db
from slor.output import *
import sys
from multiprocessing.connection import Client
from multiprocessing import Process, Pipe
import random
import time
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
    mixed_count = None
    stage_itr = None

    def __init__(self, root_config):
        self.config = root_config
        self.mixed_count = 0
        self.conn = []
        self.readmap = []
        self.stat_buf = []
        self.stage_itr = {}
        self.reread = 0

    def exec(self):

        # Print config
        box_text(config_text(self.config))

        # Gather and show driver info
        print("")
        if not self.connect_to_driver():
            self.print_message("driver(s) failed check(s), I'm out")
            sys.exit(1)

        print("")
        top_box()
        sys.stdout.write(
            "\u2502 STAGES ({}); ".format(len(self.config["tasks"]["loadorder"]))
        )
        sys.stdout.write("\n\u2502\n")
        for stage_val in self.config["tasks"]["loadorder"]:
            stage, duration = self.parse_stage_string(stage_val)
            if (
                sum(
                    opclass_from_label(stage_val) == opclass_from_label(x)
                    for x in self.config["tasks"]["loadorder"]
                )
                > 1
            ):
                self.stage_itr[opclass_from_label(stage)] = -1

        # Get database handler going
        if not self.config["no_db"]:
            self.db_sock, db_child_sock = Pipe()
            db_proc = Process(
                target=_slor_db,
                args=(db_child_sock, self.config),
            )
            db_proc.start()

        for c, stage in enumerate(self.config["tasks"]["loadorder"]):
            stage, duration = self.parse_stage_string(stage)

            if stage in ("read", "delete", "head", "mixed", "tag"):
                sys.stdout.write("\r\u2502   [   shuffling readmap...   ]   ")
                sys.stdout.flush()
                random.shuffle(self.readmap)

            elif stage == "sleep":
                box_line("     [     sleeping ({})...     ]   ".format(duration), newline=False)
                time.sleep(duration)
                continue

            elif stage == "readmap":
                self.mk_read_map()
                continue

            self.exec_stage(stage, duration)

        for c in self.conn:
            c.close()
        
        
        if not self.config["no_db"]:
            box_line("stopping db handler...")
            self.db_sock.send({"command": "STOP"})
            db_proc.join()
            sys.stdout.write("done\n")

        bottom_box()

        print("\ndone.\n")


    def connect_to_driver(self):
        ret_val = True
        self.config["driver_node_names"] = []

        top_box()
        print("\u2502 DRIVERS ({})".format(len(self.config["driver_list"])))
        print("\u2502")
        for i, hostport in enumerate(self.config["driver_list"]):
            sys.stdout.write(
                "\u2502 {0}) addr: {1}:{2};".format(
                    i + 1, hostport["host"], hostport["port"]
                )
            )
            try:
                self.conn.append(Client((hostport["host"], hostport["port"])))
                self.conn[-1].send({"command": "sysinfo"})
                resp = self.conn[-1].recv()
                self.check_driver_info(resp)

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
                "\u2502    {}warning{}} version mismatch; controller is {} driver is {}".format(
                    bcolors.WARNING, bcolors.ENDC, SLOR_VERSION, data["slor_version"]
                )
            )

        if data["sysload"][0] >= 1:
            sys.stdout.write("\n")
            sys.stdout.write(
                "\u2502    {}warning{} driver 1m sysload is > 1 ({:.2f})".format(
                    bcolors.WARNING, bcolors.ENDC, data["sysload"][0]
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
            else:
                return

        # record stage config
        if not self.config["no_db"]:
            self.db_sock.send({"stage": stage, "stage_config": workloads})

        # Distribute buckets to driver for init
        if stage == "init":
            for bc in range(0, self.config["bucket_count"]):
                workloads[bc % n_wrkrs]["bucket_list"].append(
                    "{0}{1}".format(self.config["bucket_prefix"], bc)
                )

        # Send workloads to driver
        try:
            for i, wl in enumerate(workloads):
                self.conn[i].send({"command": "workload", "config": wl})
        except BrokenPipeError:
            sys.stderr.write("driver(s) gone, exiting.\n")
            sys.exit(1)

        self.block_until_ready()
        resp = self.poll_for_response()

        self.print_message("running stage ({0})".format(stage), verbose=True)

        stats_config = copy.deepcopy(self.config)
        stats_config["mixed_profile"] = self.config["mixed_profiles"][self.mixed_count]

        self.stats_h = statHandler(stats_config, opclass_from_label(stage), duration)
        if self.get_readmap_len() > 0 or stage == "blowout":
            if stage == "blowout":
                self.stats_h.set_count_target(
                    self.config["ttl_sz_cache"] / DEFAULT_CACHE_OVERRUN_OBJ
                )
            else:
                self.stats_h.set_count_target(self.get_readmap_len())

        # If this is a prepare stage we need to be set to replace the readmap
        if stage == "prepare":
            self.new_readmap = []

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
                        self.process_message(mesg)  # Decide what to do with the mssage
                    except EOFError:
                        pass
            if donestack == 0:
                time.sleep(1)
                self.stats_h.show(final=True)
                self.print_message(
                    "threads complete for this stage ({0})".format(stage), verbose=True
                )
                break
            if self.reread > 0:
                self.stats_h.reread = self.reread
            self.stats_h.show()
            time.sleep(0.01)

        # Replace with readmap with a versioned one, save it if "save_redmap"
        # is specified and shuffle
        if stage == "prepare":
            cfg_keys = (
                "sz_range",
                "bucket_prefix",
                "bucket_count",
                "key_sz",
                "ttl_prepare_sz",
                "prepare_objects",
            )
            self.readmap = self.new_readmap

            if self.config["save_readmap"]:
                sys.stdout.write("\u2502     {}Saving readmap... ".format(bcolors.GRAY))
                sys.stdout.flush()
                save_contents = {"readmap": self.readmap}
                for cfg in cfg_keys:
                    save_contents[cfg] = self.config[cfg]

                with open(self.config["save_readmap"], "w") as fh:
                    fh.write(json.dumps(save_contents))
                    fh.close()
                sys.stdout.write("done{}\n".format(bcolors.ENDC))
                sys.stdout.flush()
            random.shuffle(self.readmap)

        if (
            opclass_from_label(stage) == "mixed"
            and self.mixed_count < len(self.config["mixed_profiles"]) - 1
        ):
            self.mixed_count += 1

        if stage != "sleep":
            self.last_stage = stage

        del self.stats_h
        self.reread = 0
        
    def block_until_ready(self):

        sys.stdout.write("\r\u2502 [waiting on worker processes...]")
        sys.stdout.flush()
        global_ready = True
        failure = False

        for i in range(0, len(self.config["driver_list"])):
            ready = self.conn[i].recv()
            if "ready" in ready and ready["ready"] != True:
                global_ready = False
            if "status" in ready and ready["status"] == "done":
                failure == True

        if failure:
            for i in range(0, len(self.config["driver_list"])):
                self.conn[i].send({"exec": True})
        if global_ready:
            for i in range(0, len(self.config["driver_list"])):
                self.conn[i].send({"exec": True})

        sys.stdout.write("\r \u2502                                  ")
        sys.stdout.flush()

    def get_readmap_len(self):
        try:
            blksz = self.config["driver_proc"] * len(self.config["driver_list"])
            objcount = (
                int(self.config["ttl_prepare_sz"] / self.config["sz_range"]["high"]) + 1
            )
        except:
            objcount = 0

        return objcount - (objcount % blksz) + blksz

    def print_message(self, message, verbose=False):
        if verbose == True and self.config["verbose"] != True:
            return
        print("\u2502 " + str(message))

    def process_message(self, message):
        """
        Filters messages from drivers and take the appropriate action
        """
        if type(message) == str:
            print(message)
        elif "command" in message:
            # what a mess
            if message["command"] == "abort":
                if "message" in message:
                    print(message["message"])
                sys.exit(0)

        elif "type" in message and message["type"] == "readmap":
            self.new_readmap += message["value"]
        elif "type" in message and message["type"] == "rereadnotice":
            if int(message["value"]) > self.reread:
                self.reread = int(message["value"])
        elif "message" in message:
            print("\u2502 message from {0}: {1}".format(message["w_id"], message["message"]))
        elif "type" in message and message["type"] == "stat":
            self.stats_h.update_standing_sample(message)
            if not self.config["no_db"]:
                self.db_sock.send(message)
        else:
            pass

    def check_status(self, mesg):
        if "status" in mesg and mesg["status"] == "done":
            return "done"

        return False

    def mk_read_map(self):
        """
        Generate bucket/key paths needed for prepared data
        """

        # config items that need to be saved/restored with the readmap
        cfg_keys = (
            "sz_range",
            "bucket_prefix",
            "bucket_count",
            "key_sz",
            "ttl_prepare_sz",
            "prepare_objects",
        )
        cfg_out = ""
        if self.config["use_readmap"]:
            with open(self.config["use_readmap"], "r") as fh:
                rmpcfg = json.load(fh)
                fh.close()
                self.readmap = rmpcfg["readmap"]
                cfg_out += "\u2502     {}: {}\n".format(
                    "length", human_readable(len(self.readmap), print_units="ops")
                )
                for cfg in cfg_keys:
                    try:
                        self.config[cfg] = rmpcfg[cfg]
                        cfg_out += "\u2502     {}: {}\n".format(cfg, self.config[cfg])
                    except:
                        sys.stderr.write(
                            "failed to find config item {} in {}\n".format(
                                cfg, self.config["use_readmap"]
                            )
                        )
            sys.stdout.write("\r\u2502" + " readmap restored from file:\n")
            sys.stdout.write(cfg_out)
        else:
            sys.stdout.write("C" + "{}{:<15}".format(" " * 26, "throughput"))
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
                        self.config["key_prefix"]
                        + gen_key(
                            key_desc=(self.config["key_sz"]["low"], self.config["key_sz"]["high"]),
                            inc=z,
                            prefix=DEFAULT_READMAP_PREFIX,
                        ),
                        []
                    )
                )
                if (z + 1) == objcount:
                    fin = True
                stat_h.readmap_progress(z, objcount, final=fin)
                

    def parse_stage_string(self, stage_str):
        items = stage_str.split("~")
        if len(items) == 2:
            return items[0], int(items[1])
        else:
            # Sleep time has it's own global config so deal with it here
            return items[0], int(self.config["sleeptime"]) if items[
                0
            ] == "sleep" else int(self.config["run_time"])

    def mk_stage(self, target, stage, wid, duration):
        """
        Create a configuration specific to the target driver
        """

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
            "run_time": duration + (DRIVER_REPORT_TIMER * 2),
            "bucket_count": int(self.config["bucket_count"]),
            "bucket_prefix": self.config["bucket_prefix"],
            "sz_range": self.config["sz_range"],
            "mpu_size": self.config["mpu_size"],
            "key_sz": self.config["key_sz"],
            "driver_list": self.config["driver_list"],
            "prepare_sz": int(
                self.config["ttl_prepare_sz"] / len(self.config["driver_list"])
            ),
            "cache_overrun_sz": int(
                self.config["ttl_sz_cache"] / len(self.config["driver_list"])
            ),
            "mixed_profile": self.config["mixed_profiles"][self.mixed_count],
            "startup_delay": (DRIVER_REPORT_TIMER / len(self.config["driver_list"])),
            "key_prefix": self.config["key_prefix"],
            "versioning": self.config["versioning"],
            "remove_buckets": self.config["remove_buckets"],
            "use_existing_buckets": self.config["use_existing_buckets"],
            "get_range": self.config["get_range"]
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
