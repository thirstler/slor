import sys
from multiprocessing.connection import Client
import random
import time
import json
from shared import *
import sqlite3
import os.path
import stat_handler


class SlorControl:

    config = None
    conn = []
    db_file = None
    db_conn = None
    db_cursor = None
    readmap = []
    stat_buf = []
    stat_viewer = None

    def __init__(self, root_config):
        self.config = root_config
        self.init_db()


    def exec(self):

        self.print_message("\nstarting up...")
        self.print_config()
        if not self.connect_to_driver():
            self.print_message("driver(s) failed check(s), I'm out")
            sys.exit(1)


        self.stat_viewer = stat_handler.rtStatViewer(self.config)

        self.print_progress_header()

        for c, stage in enumerate(self.config["tasks"]["loadorder"]):
            if stage == "blowout" and self.config["ttl_sz_cache"] == 0:
                continue
            elif stage[:5] == "sleep":
                try:
                    st = int(stage[5:])
                except:
                    st = self.config["sleeptime"]
                sys.stdout.write("sleeping ({})...".format(st))
                sys.stdout.flush()
                time.sleep(st)
                continue
            elif stage == "readmap":
                self.mk_read_map()
                continue

            self.exec_stage(stage)

        for c in self.conn:
            c.close()

    def print_progress_header(self):
        bar_width = os.get_terminal_size().columns - 78
        if bar_width > 25:
            bar_width = 25
        print("")
        print("{0:<8} {1}     {2:<15}{3:<15}{4:<13}{5:<11}{6:<10}".format(
            "Stage", " "*bar_width, "Throughput", "Bandwidth", "Resp ms", "Elapsed", "Failures"))
        print("-"*(bar_width+79))

    def print_config(self):
        twidth = os.get_terminal_size().columns
        if twidth > TERM_WIDTH_MAX: twidth = TERM_WIDTH_MAX
        print("{0}".format("-"*twidth))

        print("SLOR Configuration:")
        print("Workload name:      {0}".format(self.config["name"]))
        print("User key:           {0}".format(self.config["access_key"]))
        print("Target endpoint:    {0}".format(self.config["endpoint"]))
        print("Stage run time:     {0}s".format(self.config["run_time"]))
        print("Object size(s):     {0}".format(
            human_readable(self.config["sz_range"][0], precision=0)
            if self.config["sz_range"][0] == self.config["sz_range"][1]
            else "low: {0}, high:  {1} (avg: {2})".format(
                human_readable(self.config["sz_range"][0], precision=0),
                human_readable(self.config["sz_range"][1], precision=0),
                human_readable(self.config["sz_range"][2], precision=0)
            )))
        print("Key length(s):      {0}".format(
            self.config["key_sz"][0]
            if self.config["key_sz"][0] == self.config["key_sz"][1]
            else "low: {0}, high:  {1} (avg: {2})".format(
                self.config["key_sz"][0],
                self.config["key_sz"][1],
                self.config["key_sz"][2]
            )))
        print("Prepared objects:   {0} (readmap length)".format(
            human_readable(self.get_readmap_len(), print_units="ops")))
        print("Bucket prefix:      {0}".format(self.config["bucket_prefix"]))
        print("Num buckets:        {0}".format(self.config["bucket_count"]))
        print("Driver processes:   {0}".format(len(self.config["driver_list"])))
        print(
            "Procs per driver:   {0} ({1} driver total)".format(
                self.config["driver_proc"],
                (int(self.config["driver_proc"]) * len(self.config["driver_list"])),
            )
        )
        print(
            "Prepared data size: {0}".format(
                human_readable(self.config["ttl_prepare_sz"])
            )
        )
        print(
            "Cache overrun size: {0}".format(
                human_readable(self.config["ttl_sz_cache"])
            )
        )
        print("Stats database:     {0}".format(self.db_file))
        print("Stages:")
        stagecount = 1
        
        for i, stage in enumerate(self.config["tasks"]["loadorder"]):
            stagecount += 1
            if stage == "mixed":
                sys.stdout.write("                    {0}: {1} (".format(stagecount, stage))
                for j, m in enumerate(self.config["tasks"]["mixed_profile"]):
                    sys.stdout.write(
                        "{0}:{1}%".format(m, self.config["tasks"]["mixed_profile"][m])
                    )
                    if (j + 1) < len(self.config["tasks"]["mixed_profile"]):
                        sys.stdout.write(", ")
                print(")")
            elif stage == "readmap":
                print("{} {}: {}".format(" "*22, stagecount, " readmap - generate keys for use during read operations"))
            elif stage == "init":
                print("{} {}: {}".format(" "*22, stagecount, " init - create needed buckets/config"))
            elif stage == "prepare":
                print("{} {}: {}".format(" "*22, stagecount, " prepare - write objects needed for read operations"))
            elif stage == "blowout":
                if self.config["ttl_sz_cache"] == 0:
                    stagecount -= 1
                    continue
                else:
                    print("{} {}: {}".format(" "*22, stagecount, " blowout - overrun page cache (needs proper config)"))
            elif stage == "read":
                print("{} {}: {}".format(" "*22, stagecount, " read - pure GET workload"))
            elif stage == "write":
                print("{} {}: {}".format(" "*22, stagecount, " write - pure PUT workload"))
            elif stage == "head":
                print("{} {}: {}".format(" "*22, stagecount, " head - pure HEAD workload"))
            elif stage == "delete":
                print("{} {}: {}".format(" "*22, stagecount, " delete - pure DELETE workload"))
            elif stage == "tag":
                print("{} {}: {}".format(" "*22, stagecount, " tag - pure tagging (metadata) workload"))


    def connect_to_driver(self):
        ret_val = True
        self.config["driver_node_names"] = []
        twidth = os.get_terminal_size().columns
        if twidth > TERM_WIDTH_MAX: twidth = TERM_WIDTH_MAX
        
        print("Drivers ({})".format(len(self.config["driver_list"])))
        print("{0}".format("-"*twidth))
        for i, hostport in enumerate(self.config["driver_list"]):
            sys.stdout.write(
                "{0}) addr: {1}:{2};".format(i+1, hostport["host"], hostport["port"])
            )
            #try:
            self.conn.append(Client((hostport["host"], hostport["port"])))
            self.conn[-1].send({"command": "sysinfo"})
            resp = self.conn[-1].recv()
            self.check_driver_info(resp)
            self.mk_data_store(resp)

            #except Exception as e:
            #    sys.stderr.write(
            #        "  unexpected exception ({2}) connecting to {0}:{1}, exiting\n".format(
            #            hostport["host"], hostport["port"], e
            #        )
            #    )
            #    ret_val = False

        return ret_val

    def check_driver_info(self, data):

        self.config["driver_node_names"].append(data["uname"].node)

        sys.stdout.write(" hostname: {0};".format(data["uname"].node))
        sys.stdout.write(
            " OS: {0} release {1}".format(data["uname"].system, data["uname"].release)
        )
        if data["slor_version"] != SLOR_VERSION:
            sys.stdout.write("\n")
            print(
                "  \033[1;31mwarning\033[0m: version mismatch; controller is {0} driver is {1}".format(
                    SLOR_VERSION, data["slor_version"]
                )
            )

        if data["sysload"][0] >= 1:
            sys.stdout.write("\n")
            sys.stdout.write(
                "  \033[1;31mwarning\033[0m: driver 1m sysload is > 1 ({0})".format(
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
                    "  \033[1;31mwarning\033[0m: iface {0} has dropped packets or errors".format(
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

        # Set up real-time stats view
        self.stat_viewer.set_stage(stage)
        if stage == "blowout":
            self.stat_viewer.set_progress_count(
                int(self.config["ttl_sz_cache"] / DEFAULT_CACHE_OVERRUN_OBJ))
        elif stage in PROGRESS_BY_COUNT:
            self.stat_viewer.set_progress_count(len(self.readmap))
        elif stage in PROGRESS_BY_TIME:
            self.stat_viewer.set_progress_time(self.config["run_time"])

        # Send workloads to driver
        for i, wl in enumerate(workloads):
            self.conn[i].send({"command": "workload", "config": wl})

        self.print_message("running stage ({0})".format(stage), verbose=True)

        """
        Primary Loop - monitors the drivers by polling messages from them
        """
        donestack = len(workloads)
        while True:
            group_time = time.time()
            
            for i, wl in enumerate(workloads):
                while self.conn[i].poll():
                    try:
                        mesg = self.conn[i].recv()
                        if self.check_status(mesg) == "done":
                            donestack -= 1
                        self.process_message(mesg)    # Decide what to do with the mssage
                    except EOFError:
                        pass

            self.stat_viewer.show(SHOW_STATS_RATE, now=group_time)

            if donestack == 0:
                time.sleep(1)
                self.stat_viewer.show(SHOW_STATS_RATE, now=group_time, final=True)
                self.print_message(
                    "threads complete for this stage ({0})".format(stage), verbose=True
                )
                break

            time.sleep(0.01)

    def get_readmap_len(self):
        try:
            blksz = (self.config["driver_proc"] * len(self.config["driver_list"]))
            objcount = int(self.config["ttl_prepare_sz"] / self.config["sz_range"][2]) + 1
        except:
            objcount = 0

        return (objcount - (objcount % blksz) + blksz)


    def mk_read_map(self, key_desc={"min": 40, "max": 40}):

        if len(self.readmap) > 0:
            self.print_message(
                "readmap of {0} entries exists".format(len(self.readmap))
            )
            return
        
        objcount = self.get_readmap_len()
        

        self.stat_viewer.set_stage("readmap")
        self.stat_viewer.set_progress_count(objcount)

        skipcount = int(objcount/100)
        samplecount = 0
        timer = time.time()
        avgrate = []
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
            samplecount += 1
            if z % skipcount == 0 or objcount == (z+1):
                final = False
                now = time.time()
                avgrate.append(samplecount/(now - timer))
                if objcount == (z+1):
                    final = True
                    ttl=0
                    for i in avgrate: ttl += i
                    avgrate.append(ttl/len(avgrate))
                self.stat_viewer.arbitrary_progress(z, objcount, ops=avgrate[-1], title="readmap", final=final)
                samplecount = 0
                timer = now

    def print_message(self, message, verbose=False):
        if verbose == True and self.config["verbose"] != True:
            return
        print(str(message))


    def process_message(self, message):
        """
        Filters messages from drivers and take the appropriate action
        """
        try:
            if type(message) == str:
                print(message)
            elif "message" in message:
                print("{0}: {1}".format(message["w_id"], message["message"]))
            elif "type" in message and message["type"] == "stat":
                self.store_stat(message)
            else:
                pass  # ignore
        except Exception as e:
            print(str(e))

    def check_status(self, mesg):
        if "status" in mesg and mesg["status"] == "done":
            return "done"

        return False

    def store_stat(self, message):

        # Maintain some data in-memory for real-time visibility
        self.stat_viewer.store(message)
        
        # Add to database for analysis later
        sql = "INSERT INTO {0} VALUES ({1}, {2}, '{3}', '{4}')".format(
            message["w_id"].replace(".", "_"),
            message["t_id"],
            message["time"],
            message["stage"],
            json.dumps(message),
        )
        self.db_cursor.execute(sql)
        self.db_conn.commit()

    def init_db(self):

        if self.db_conn == None:
            self.db_file = "{0}/{1}.db".format(STATS_DB_DIR, self.config["name"])
            vcount = 1
            while os.path.exists(self.db_file):
                self.db_file = "{0}/{1}_{2}.db".format(
                    STATS_DB_DIR, self.config["name"], vcount
                )
                vcount += 1
            self.db_conn = sqlite3.connect(self.db_file)
            self.db_cursor = self.db_conn.cursor()

    def mk_data_store(self, sysinf):

        self.db_cursor.execute(
            "CREATE TABLE {0} (t_id INT, ts INT, stage STRING, data JSON)".format(
                sysinf["uname"].node.replace(".", "_")
            )
        )
        self.db_cursor.execute(
            "CREATE INDEX {0}_ts_i ON {0} (ts)".format(sysinf["uname"].node.replace(".", "_"))
        )
        # self.db_cursor.execute(
        #    "CREATE INDEX {0}_stage_i ON {0} (stage)".format(sysinf["uname"].node)
        # )

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
            "prepare_sz": int(
                self.config["ttl_prepare_sz"] / len(self.config["driver_list"])
            ),
            "cache_overrun_sz": int(
                self.config["ttl_sz_cache"] / len(self.config["driver_list"])
            )
        }

        # Work out the readmap slices
        sys.stdout.write("\r  [shuffling readmap...]  ")
        random.shuffle(self.readmap)
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

        return config
