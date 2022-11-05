from slor.shared import *
from slor.stat_handler import statHandler
from slor.slor_db import _slor_db
from slor.output import *
from slor.screen import SlorScreen
from slor.plots import *
import sys
from multiprocessing.connection import Client
from multiprocessing import Process, Pipe
import random
import time
import json
import copy
import curses

class PeerCheckFailure(Exception):
    pass

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
    stage_itr = None
    screen = None

    def __init__(self, root_config):
        self.config = root_config
        self.conn = []
        self.readmap = []
        self.stat_buf = []
        self.stage_itr = {}
        self.reread = 0


    def exec(self, stdscr):
        self.screen = SlorScreen(stdscr)
        self.screen.set_title("Startup")
        if self.config["no_plot"]:
            self.screen.set_footer(BANNER)
        else:
            self.screen.set_footer("reset (h)istogram scale, (a)bort" )

        # Label the stages in the load order
        indexes = {}
        load_order = []
        for i, task in enumerate(self.config["tasks"]["loadorder"]):
            if task not in indexes:
                indexes[task] = 0
            else:
                indexes[task] += 1
            load_order.append("{}:{}".format(task, indexes[task]))
        self.config["tasks"]["loadorder"] = load_order

        dr_status, dr_status_text = self.connect_to_driver()
        self.screen.set_data("\n "+dr_status_text, append=True)
        self.screen.refresh()

        time.sleep(1)
        
        #if not dr_status:
        #    raise PeerCheckFailure

        # Get database handler going
        if not self.config["no_db"]:
            self.db_sock, db_child_sock = Pipe()
            self.db_proc = Process(
                target=_slor_db,
                args=(db_child_sock, self.config),
            )
            self.db_proc.start()

        for c, stage in enumerate(load_order):
            duration = self.config["run_time"]
            stage_class = opclass_from_label(stage)
            self.screen.set_title("Run stage {}: {}".format(c, stage))

            if stage_class in ("read", "delete", "head", "mixed", "tag"):
                self.screen.set_message(message="shuffling readmap...")
                self.screen.refresh()
                random.shuffle(self.readmap)

            elif stage_class == "sleep":
                self.screen.set_message(message="sleeping ({})...".format(duration))
                self.screen.refresh()
                time.sleep(duration)
                self.screen.clear_message()
                self.screen.refresh()
                continue

            elif stage_class == "readmap":
                self.mk_read_map()
                continue

            supplement = (
                self.config["tasks"]["config_supplements"][stage_class]
                if stage_class in self.config["tasks"]["config_supplements"]
                else None
            )
            self.exec_stage(stage, duration, supplement)

        for c in self.conn:
            c.close()

        if not self.config["no_db"]:
            self.screen.set_message(message="stopping db handeler...")
            self.screen.refresh()
            self.db_sock.send({"command": "STOP"})
            self.db_proc.join()
            sys.stdout.write("done\n")

        print("\ndone.\n")

    def connect_to_driver(self):
        ret_val = True
        self.config["driver_node_names"] = []
        status_text = ''
        
        status_text += "DRIVERS ({})\n".format(len(self.config["driver_list"]))
        
        for i, hostport in enumerate(self.config["driver_list"]):
            status_text += " {0}) addr: {1}:{2};".format(
                    i + 1, hostport["host"], hostport["port"]
                )
            try:
                self.conn.append(Client((hostport["host"], hostport["port"])))
                self.conn[-1].send({"command": "sysinfo"})
                resp = self.conn[-1].recv()
                status_text += " " + self.check_driver_info(resp)

            except Exception as e:
                status_text += " unexpected exception ({2}) connecting to {0}:{1}, exiting\n".format(
                        hostport["host"], hostport["port"], e
                    )
                ret_val = False

        return ret_val, status_text

    def check_driver_info(self, data):
        return_text = ''

        self.config["driver_node_names"].append(data["uname"].node)

        return_text += " hostname: {0};".format(data["uname"].node)
        return_text += " OS: {0}".format(data["uname"].system)
        
        if data["slor_version"] != SLOR_VERSION:
            return_text += '\n'
            return_text += "warning version mismatch; controller is {} driver is {}".format(
                    SLOR_VERSION, data["slor_version"]
                )

        if data["sysload"][0] >= 1:
            return_text += '\n'
            sreturn_text += "warning driver 1m sysload is > 1 ({:.2f})".format(
                    data["sysload"][0]
                )
        return_text += '\n'
        return return_text

    def poll_for_response(self, all=True):
        return_msg = []

        for i in range(0, len(self.config["driver_list"])):
            while self.conn[i].poll():
                return_msg.append(self.conn[i].recv())
        return return_msg

    def exec_stage(self, stage, duration, supplement):
        """
        Contains the main loop communicating with driver processes. Presentation and
        control logic: two great tastes that taste great together!
        """
        stage_class = opclass_from_label(stage)
        workload_index = int(stage[stage.find(":") + 1 :])
        workloads = []
        n_wrkrs = len(self.config["driver_list"])

        # Create the workloads
        for wc, target in enumerate(self.config["driver_list"]):
            stage_cfg = self.mk_stage(target, stage, wc, duration, supplement)
            if stage_cfg:
                workloads.append(stage_cfg)
            else:
                return

        # record stage config to stats db. Remove readmap, takes a lot of space
        # and is not necessary
        if not self.config["no_db"]:
            send_copy = []
            for work in workloads:
                send_copy.append(copy.deepcopy(work))
                if "readmap" in send_copy[-1]:
                    del send_copy[-1]["readmap"]

            try:
                self.db_sock.send({"stage": stage, "stage_config": send_copy})
            except:
                sys.stdout.write("\r\u2502 error sending stats to db process")
                sys.stdout.flush()
                pass

            del send_copy

        # Distribute buckets to driver for init
        if stage_class == "init":
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
        self.poll_for_response()

        self.screen.set_title(stage)
        self.screen.refresh()

        # Replace mixed profile with the proper one
        stats_config = copy.deepcopy(self.config)
        stats_config["mixed_profile"] = self.config["mixed_profiles"][workload_index]

        self.stats_h = statHandler(stats_config, stage, duration, workload_index=workload_index)
        if self.get_readmap_len() > 0 or stage_class == "blowout":
            if stage_class == "blowout":
                self.stats_h.set_count_target(
                    self.config["ttl_sz_cache"] / DEFAULT_CACHE_OVERRUN_OBJ
                )
            else:
                self.stats_h.set_count_target(self.get_readmap_len())

        # If this is a prepare stage we need to be set to replace the readmap
        if stage_class == "prepare":
            self.new_readmap = []

        """
        Primary Loop - monitors the drivers by polling messages from them
        """
        donestack = len(workloads)
        while True:
            for i, wl in enumerate(workloads):
                while self.conn[i].poll():
                    try:
                        mesg = self.conn[i].recv()
                        if self.check_status(mesg) == "done":
                            donestack -= 1
                        self.process_message(mesg)  # Decide what to do with the message
                    except EOFError:
                        pass
            if donestack == 0:
                time.sleep(1)
                self.stats_h.show(self.screen, final=True)
                self.screen.refresh()
                break

            if self.reread > 0:
                self.stats_h.reread = self.reread

            self.stats_h.show(self.screen)
            self.screen.refresh()
            
            # Doesn't really matter how long, just long enough to keep the loop
            # from getting wrapped tight.
            time.sleep(0.01) 

        # Replace readmap with a versioned one, save it if "save_readmap"
        # is specified and shuffle
        if stage_class == "prepare":
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

        del self.stats_h
        self.reread = 0

    def block_until_ready(self):

        self.screen.set_message(" waiting on worker processes...")
        self.screen.refresh()
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

        #self.screen.clear_message()
        #self.screen.refresh()

    def get_readmap_len(self):
        blksz = self.config["driver_proc"] * len(self.config["driver_list"])
        objcount = int(self.config["prepare_objects"]) + 1

        return objcount - (objcount % blksz) + blksz

    def print_message(self, message, verbose=False):
        if verbose == True and self.config["verbose"] != True:
            return
        self.screen.data_win.addstr(str(message))
        self.screen.refresh()

    def process_message(self, message):
        """
        Filters messages from drivers and take the appropriate action
        """
        if type(message) == str:
            print(message)
        elif "command" in message:
            # what a fucking mess
            if message["command"] == "abort":
                if "message" in message:
                    curses.endwin()
                    sys.stderr.write("\n{}\n".format(message["message"]))
                try:
                    self.db_proc.kill()
                except:
                    pass
                sys.exit(0)

        elif "type" in message and message["type"] == "readmap":
            self.new_readmap += message["value"]
        elif "type" in message and message["type"] == "rereadnotice":
            if int(message["value"]) > self.reread:
                self.reread = int(message["value"])
        elif "message" in message:
            self.screen.data_win.addstr(
                "\n message from {0}: {1}".format(
                    message["w_id"], message["message"]
                )
            )
            self.screen.refresh()
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
            self.screen.data_win.addstr("readmap restored from file:")
            self.screen.set_data(cfg_out, append=True) # use set_data since it might scroll
        else:
            objcount = self.get_readmap_len()
            for z in range(0, objcount):

                self.readmap.append(
                    (
                        "{0}{1}".format(
                            self.config["bucket_prefix"],
                            random.randrange(0, self.config["bucket_count"]),
                        ),
                        self.config["key_prefix"]
                        + gen_key(
                            key_desc=(
                                self.config["key_sz"]["low"],
                                self.config["key_sz"]["high"],
                            ),
                            inc=z,
                            prefix=DEFAULT_READMAP_PREFIX,
                        ),
                        [],
                    )
                )

                if z % 1000 == 0 or (z + 1) == objcount:
                    self.screen.set_progress(percent=(z/objcount))
                    self.screen.refresh()


    def mk_stage(self, target, stage, wid, duration, supplement):
        """
        Create a configuration specific to the target driver
        """
        stage_class = opclass_from_label(stage)
        load_index = int(stage[stage.find(":") + 1 :])

        # Overrideable config
        procs = self.config["driver_proc"]
        if supplement != None:
            if "processes" in supplement:
                procs = supplement["processes"]


        # Base config for every stage, kind of shitty because most of this is
        # just pass-thu
        config = {
            "host": target["host"],
            "port": target["port"],
            "w_id": wid,
            "threads": procs,
            "access_key": self.config["access_key"],
            "secret_key": self.config["secret_key"],
            "endpoint": self.config["endpoint"],
            "verify": self.config["verify"],
            "region": self.config["region"],
            "run_time": duration + (DRIVER_REPORT_TIMER * 2),
            "bucket_count": int(self.config["bucket_count"]),
            "bucket_prefix": self.config["bucket_prefix"],
            "sz_range": self.config["sz_range"],
            "random_from_pool": self.config["random_from_pool"],
            "compressible": self.config["compressible"],
            "mpu_size": self.config["mpu_size"],
            "key_sz": self.config["key_sz"],
            "driver_list": self.config["driver_list"],
            "prepare_sz": int(
                self.config["ttl_prepare_sz"] / len(self.config["driver_list"])
            ),
            "cache_overrun_sz": int(
                self.config["ttl_sz_cache"] / len(self.config["driver_list"])
            ),
            "mixed_profile": self.config["mixed_profiles"][load_index],
            "startup_delay": (DRIVER_REPORT_TIMER / len(self.config["driver_list"])),
            "key_prefix": self.config["key_prefix"],
            "versioning": self.config["versioning"],
            "remove_buckets": self.config["remove_buckets"],
            "use_existing_buckets": self.config["use_existing_buckets"],
            "get_range": self.config["get_range"],
            "label": stage,
            "no_plot": self.config["no_plot"]
        }

        # Work out the readmap slices
        chunk = int(len(self.readmap) / len(self.config["driver_list"]))
        offset = chunk * wid
        end_slice = offset + chunk
        mapslice = self.readmap[offset:end_slice]

        if stage_class == "init":
            config["type"] = "init"
            config["bucket_list"] = []
        if stage_class == "prepare":
            config["type"] = "prepare"
            config["readmap"] = mapslice
        if stage_class == "blowout":
            config["type"] = "blowout"
        if stage_class == "read":
            config["type"] = "read"
            config["readmap"] = mapslice
        if stage_class == "write":
            config["type"] = "write"
        if stage_class == "mixed":
            config["type"] = "mixed"
            config["readmap"] = mapslice
        if stage_class == "delete":
            config["type"] = "delete"
            config["readmap"] = mapslice
        if stage_class == "head":
            config["type"] = "head"
            config["readmap"] = mapslice
        if stage_class == "cleanup":
            config["type"] = "cleanup"

        return config
