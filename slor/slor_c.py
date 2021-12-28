import sys
from multiprocessing.connection import Client
import random
import time
import json
from shared import *
import sqlite3
import os.path
import uuid
import stat_handler


class SlorControl:

    config = None
    conn = []
    db_file = None
    db_conn = None
    db_cursor = None
    readmap = []
    stat_buf = []

    def __init__(self, root_config):
        self.config = root_config
        self.init_db()

    def exec(self):

        self.print_message("\nstarting up...\n")
        self.print_config()
        if not self.connect_to_workers():
            self.print_message("worker(s) failed check(s), I'm out")
            sys.exit(1)

        self.mk_read_map()

        for stage in self.config["tasks"]["loadorder"]:
            self.exec_stage(stage)

        for c in self.conn:
            c.close()

    def print_config(self):
        print(
            "################################################################################"
        )
        print("# SLOR Configuration:")
        print("Workload name:      {0}".format(self.config["name"]))
        print("User key:           {0}".format(self.config["access_key"]))
        print("Target endpoint:    {0}".format(self.config["endpoint"]))
        print("Stage run time:     {0}s".format(self.config["run_time"]))
        print("Bucket prefix:      {0}".format(self.config["bucket_prefix"]))
        print("Num buckets:        {0}".format(self.config["bucket_count"]))
        print("Driver processes:   {0}".format(len(self.config["worker_list"])))
        print(
            "Procs per driver:   {0} ({1} workers)".format(
                self.config["worker_thr"],
                (int(self.config["worker_thr"]) * len(self.config["worker_list"])),
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
        for i, stage in enumerate(self.config["tasks"]["loadorder"]):
            if stage == "mixed":
                sys.stdout.write("                    {0}: {1} (".format(i, stage))
                for j, m in enumerate(self.config["tasks"]["mixed_profile"]):
                    sys.stdout.write(
                        "{0}:{1}%".format(m, self.config["tasks"]["mixed_profile"][m])
                    )
                    if (j + 1) < len(self.config["tasks"]["mixed_profile"]):
                        sys.stdout.write(", ")
                print(")")
            else:
                print("                    {0}: {1}".format(i, stage))
        print()
        print()

    def connect_to_workers(self):
        ret_val = True
        for hostport in self.config["worker_list"]:
            print(
                "################################################################################"
            )
            print(
                "# worker system ({0}:{1})".format(hostport["host"], hostport["port"])
            )
            try:
                self.conn.append(Client((hostport["host"], hostport["port"])))
                self.conn[-1].send({"command": "sysinfo"})
                resp = self.conn[-1].recv()
                self.check_worker_info(resp)
                self.mk_data_store(resp)

            except Exception as e:
                sys.stderr.write(
                    "  unexpected exception ({2}) connecting to {0}:{1}, exiting\n".format(
                        hostport["host"], hostport["port"], e
                    )
                )
                ret_val = False

        return ret_val

    def check_worker_info(self, data):
        print("  Hostname: {0}".format(data["uname"].node))
        print(
            "  OS: {0} release {1}".format(data["uname"].system, data["uname"].release)
        )
        if data["slor_version"] != SLOR_VERSION:
            print(
                "  \033[1;31mwarning\033[0m: version mismatch; controller is {0} worker is {1}".format(
                    SLOR_VERSION, data["slor_version"]
                )
            )
        if data["sysload"][0] >= 1:
            print(
                "  \033[1;31mwarning\033[0m: worker 1m sysload is > 1 ({0})".format(
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
                print(
                    "  \033[1;31mwarning\033[0m: iface {0} has dropped packets or errors".format(
                        iface
                    )
                )

    def exec_stage(self, stage):
        """
        Contains the main loop communicating with worker processes
        """

        workloads = []
        n_wrkrs = len(self.config["worker_list"])

        # Create the workloads
        for wc, target in enumerate(self.config["worker_list"]):
            stage_cfg = self.mk_stage(target, stage, wc)
            if stage_cfg:
                workloads.append(stage_cfg)
            else:
                return

        # Distribute buckets to workers for init
        if stage == "init":
            for bc in range(0, self.config["bucket_count"]):
                workloads[bc % n_wrkrs]["bucket_list"].append(
                    "{0}{1}".format(self.config["bucket_prefix"], bc)
                )

        # Fire!
        for i, wl in enumerate(workloads):
            self.conn[i].send({"command": "workload", "config": workloads[i]})

        if stage == "prepare":  # Database derived status handler
            stat_view = stat_handler.dbStats(
                self.db_file, SHOW_STATS_EVERY, len(self.readmap), stage
            )
        elif stage == "read" or stage == "write" or stage == "mixed":
            stat_view = stat_handler.timingStats(
                SHOW_STATS_EVERY, time.time(), self.config["run_time"], stage=stage
            )
        else:
            self.print_message("stage: {0}".format(stage))
            stat_view = stat_handler.textStats(stage=stage)

        self.print_message("running stage ({0})".format(stage), verbose=True)

        """
        Primary Loop - monitors the workers by polling messages from them
        """
        donestack = len(workloads)
        while True:

            for i, wl in enumerate(workloads):
                while self.conn[i].poll():
                    try:
                        mesg = self.conn[i].recv()
                        if self.check_status(mesg) == "done":
                            donestack -= 1
                        self.process_message(mesg)
                    except EOFError:
                        pass

            stat_view.trigger()

            if donestack == 0:
                time.sleep(1)
                stat_view.trigger(final=True)
                self.print_message(
                    "threads complete for this stage ({0})".format(stage), verbose=True
                )
                break

            time.sleep(0.01)

    def mk_read_map(self, key_desc={"min": 40, "max": 40}):

        if len(self.readmap) > 0:
            self.print_message(
                "readmap of {0} entries exists".format(len(self.readmap))
            )
            return

        objcount = int(self.config["ttl_prepare_sz"] / self.config["sz_range"][2]) + 1

        sys.stdout.write("building readmap ({0} entries)... ".format(objcount))
        sys.stdout.flush()

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

        self.print_message("done")

    def print_message(self, message, verbose=False):
        if verbose == True and self.config["verbose"] != True:
            return
        print(str(message))

    def process_message(self, message):
        """
        Filters messages from workers and take the appropriate action
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
        sql = "INSERT INTO {0} VALUES ({1}, {2}, '{3}', '{4}')".format(
            message["w_id"],
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
                sysinf["uname"].node
            )
        )
        self.db_cursor.execute(
            "CREATE INDEX {0}_ts_i ON {0} (ts)".format(sysinf["uname"].node)
        )
        # self.db_cursor.execute(
        #    "CREATE INDEX {0}_stage_i ON {0} (stage)".format(sysinf["uname"].node)
        # )

    def mk_stage(self, target, stage, wid):

        # Base config for every stage
        config = {
            "host": target["host"],
            "port": target["port"],
            "threads": self.config["worker_thr"],
            "access_key": self.config["access_key"],
            "secret_key": self.config["secret_key"],
            "endpoint": self.config["endpoint"],
            "verify": self.config["verify"],
            "region": self.config["region"],
            "run_time": self.config["run_time"],
            "bucket_count": self.config["bucket_count"],
            "bucket_prefix": self.config["bucket_prefix"],
            "sz_range": self.config["sz_range"],
            "prepare_sz": int(
                self.config["ttl_prepare_sz"] / len(self.config["worker_list"])
            ),
        }

        # Work out the readmap slices
        random.shuffle(self.readmap)
        slice_width = len(self.readmap) / len(self.config["worker_list"])
        offset = int(slice_width * wid)
        end_slice = int(offset + slice_width)
        mapslice = self.readmap[
            offset : end_slice if end_slice < len(self.readmap) else -1
        ]

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
