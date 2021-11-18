import sys
import argparse
from multiprocessing.connection import Client
import random
import time
import json
from shared import *
import sqlite3
import os.path
import uuid


class SlorControl:

    config = None
    conn = []
    db_conn = None
    db_cursor = None
    readmap = []

    def __init__(self, root_config):
        self.config = root_config
        self.init_db()

    def exec(self):

        self.log("\nstarting up...\n")
        self.print_config()
        if not self.connect_to_workers():
            self.log("worker(s) failed check(s), I'm out")
            sys.exit(1)

        self.mk_read_map()

        for stage in self.config["tasks"]["loadorder"]:
            self.exec_stage(stage)

        for c in self.conn:
            c.close()

    def print_config(self):
        print("SLOR Configuration:")
        print("Workload name:      {0}".format(self.config["name"]))
        print("User key:           {0}".format(self.config["access_key"]))
        print("Target endpoint:    {0}".format(self.config["endpoint"]))
        print("Stage run time:     {0}s".format(self.config["run_time"]))
        print("Bucket prefix:      {0}".format(self.config["bucket_prefix"]))
        print("Num buckets:        {0}".format(self.config["bucket_count"]))
        print("Driver processes:   {0}".format(len(self.config["worker_list"])))
        print("Procs per driver:   {0}".format(self.config["worker_thr"]))
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
        print("Task list:          {0}".format(self.config["tasks"]))
        print()
        print()

    def log(self, message):
        if LOG_TO_CONSOLE:
            sys.stdout.write("{0}\n".format(message))
            sys.stdout.flush()
        else:
            pass  # fuck, I don't know...

    def connect_to_workers(self):
        ret_val = True
        for hostport in self.config["worker_list"]:
            self.log(
                "=== worker system '{0}:{1}' ===".format(
                    hostport["host"], hostport["port"]
                )
            )
            try:
                self.conn.append(Client((hostport["host"], hostport["port"])))
                self.conn[-1].send({"command": "sysinfo"})
                resp = self.conn[-1].recv()
                self.log(self.check_worker_info(resp))
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

        """ Primary Loop"""
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

            if donestack == 0:
                print("Threads complete for this workload ({0})".format(stage))
                break

            time.sleep(0.01)

    def mk_read_map(self):

        if len(self.readmap) > 0:
            print("readmap of {0} entries exists".format(len(self.readmap)))
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
                    "{0}/{1}".format(OBJECT_PREFIX_LOC, str(uuid.uuid4()), False),
                )
            )

        print("done")

    def process_message(self, message):
        if "type" in message and message["type"] == "stat":
            print(message)
            self.store_stat(message)
        else:
            print(message)

        return True

    def check_status(self, mesg):
        if "status" in mesg and mesg["status"] == "done":
            return "done"

        return False

    def store_stat(self, message):
        sql = "INSERT INTO {0} VALUES ({1}, {2}, '{3}', '{4}')".format(
            message["w_id"],
            message["t_id"],
            message["time"],
            message["key"],
            json.dumps(message),
        )
        self.db_cursor.execute(sql)
        self.db_conn.commit()

    def init_db(self):

        if self.db_conn == None:
            db_file = "/var/tmp/{0}.db".format(self.config["name"])
            vcount = 1
            while os.path.exists(db_file):
                db_file = "/var/tmp/{0}_{1}.db".format(self.config["name"], vcount)
                vcount += 1
            self.db_conn = sqlite3.connect(db_file)
            self.db_cursor = self.db_conn.cursor()

    def mk_data_store(self, sysinf):
        self.db_cursor.execute(
            "CREATE TABLE {0} (t_id int, ts int, key string, data json)".format(
                sysinf["uname"].node
            )
        )

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

    def log_messages(self, mesg):
        pass

    def process_worker_output(self, mesg):
        if "message" in mesg:
            self.log_messages(mesg)


###############################################################################
###############################################################################
## Conifg generation tasks
##
def calc_prepare_size(sizerange, runtime, iops):
    if len(sizerange) > 1:
        avgsz = ((sizerange[1] - sizerange[0]) / 2) + sizerange[0]
    else:
        avgsz = sizerange[0]

    return avgsz * iops * runtime


def parse_size_range(stringval):

    if not "-" in stringval:
        sz = parse_size(stringval)
        return (sz, sz, sz)

    else:
        vals = stringval.split("-")
        low = int(parse_size(vals[0].strip()))
        high = int(parse_size(vals[1].strip()))
        avg = (low + high) / 2
        return (low, high, avg)


def parse_worker_list(stringval):
    hostlist = []
    for hostport in stringval.split(","):
        if ":" in hostport:
            host = hostport.split(":")[0]
            port = int(hostport.split(":")[1])
        else:
            host = hostport
            port = int(DEFAULT_WORKER_PORT)
        hostlist.append({"host": host, "port": port})
    return hostlist


def generate_tasks(args):

    choose_any = ("prepare", "init", "read", "write", "delete", "head", "mixed")
    loads = tuple(args.loads.split(","))
    mix_prof_obj = {}
    for l in loads:
        if l not in choose_any:
            sys.stderr.write('"{0}" is not a load option\n'.format(l))
            sys.exit(1)

    if "mixed" in loads:
        perc = 0
        mix_prof_obj = json.loads(args.mix_profile)
        for l in mix_prof_obj:
            perc += float(mix_prof_obj[l])
        if perc != 100:
            sys.stderr.write("your mixed load profile vaules don't equal 100\n")
            sys.exit(1)

    # Arrange the load order
    actions = ("init",)
    if len(loads) == 1 and "write" in loads:
        actions += ("write",)
    else:
        actions += ("prepare",)
    if "write" in loads:
        actions += ("write",)
    if "read" in loads:
        actions += ("blowout", "read")
    if "head" in loads:
        actions += ("head",)
    if "mixed" in loads:
        actions += ("blowout", "mixed")
    if "delete" in loads:
        actions += ("delete",)  # debateable, might want cache overwritten as well?

    return {"loadorder": actions, "mixed_profile": mix_prof_obj}


def run():
    parser = argparse.ArgumentParser(
        description="Slor (S3 Load Ruler) is a distributed load generation and benchmarking tool for S3 storage"
    )
    parser.add_argument("controller")  # Make argparse happy
    parser.add_argument(
        "--name", default="generic", help="name for this workload/benchmark"
    )
    parser.add_argument(
        "--bucket-prefix",
        default=DEFAULT_BUCKET_PREFIX,
        help='prefix to use when creating buckets (defaults to "{0}")'.format(
            DEFAULT_BUCKET_PREFIX
        ),
    )
    parser.add_argument("--profile", default=DEFAULT_PROFILE_DEF)
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    parser.add_argument(
        "--verify",
        default=True,
        help='verify HTTPS certs, defaults to "true"; set to "false" or a path to a CA bundle (bundle needs to be present on all worker hosts',
    )
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--access-key", default=False)
    parser.add_argument("--secret-key", default=False)
    parser.add_argument(
        "--stage-time",
        default=DEFAULT_BENCH_LEN,
        help="how long each load stage should run (default: {0} seconds)".format(
            DEFAULT_BENCH_LEN
        ),
    )
    parser.add_argument(
        "--object-size",
        default=DEFAULT_OBJECT_SIZE,
        help="object size to use; accepts values with common suffexes (1MB, 1MiB) and ranges (1KB-12MiB) - defaults to {0}".format(
            DEFAULT_OBJECT_SIZE
        ),
    )
    parser.add_argument(
        "--worker-list",
        default=DEFAULT_WORKER_LIST,
        help="camma-delimited list of running worker hosts (in host:port format); 9256 is assumed if port is excluded; local worker is launched if unspecified",
    )
    parser.add_argument(
        "--worker-threads",
        default=DEFAULT_SESSION_COUNT,
        help="number of simultanious HTTP sessions per worker - num_workers * num_threads will equal total thread count (defaults to {0})".format(
            DEFAULT_SESSION_COUNT
        ),
    )
    parser.add_argument(
        "--cachemem-size",
        default="0",
        help="total amount of memory available in the target cluster for page cache and read caches; post-prepare stage will write this value to overwhelm caches for a cold read stage",
    )
    parser.add_argument(
        "--iop-limit",
        default=DEFAULT_UPPER_IOP_LIMIT,
        help="maximum expected IOP/s value that you can expect to hit given the workload; needed to determine the size of the prepare data given the load run-time",
    )
    parser.add_argument(
        "--loads",
        default=DEFAULT_TESTS,
        help="specify the loads you want to run; any (or all) of read, write, delete, head, mixed",
    )
    parser.add_argument(
        "--mix-profile",
        default=DEFAULT_MIX_PROFILE,
        help="profile of mixed load percentages in JASON format, eg: '{0}'".format(
            DEFAULT_MIX_PROFILE
        ),
    )
    parser.add_argument(
        "--bucket-count",
        default=DEFAULT_BUCKET_COUNT,
        help="number of buckets to distribute over, defaults to '{0}'".format(
            DEFAULT_BUCKET_COUNT
        ),
    )
    args = parser.parse_args()

    # if no cmd line args, get from profile, then env (in that order)
    if not args.access_key and not args.secret_key:
        args.access_key, args.secret_key = get_profile(args.profile)

    # Must be AWS if no endpoint is given, to keep the boto3 easy we need to
    # construct the AWS endpoint explicitly.
    if args.endpoint == "":
        args.endpoint = "https://s3.{0}.amazonaws.com".format(args.region)

    tasks = generate_tasks(args)

    root_config = {
        "name": args.name,
        "access_key": args.access_key,
        "secret_key": args.secret_key,
        "endpoint": args.endpoint,
        "verify": args.verify,
        "region": args.region,
        "sz_range": parse_size_range(args.object_size),
        "run_time": int(args.stage_time),
        "bucket_count": int(args.bucket_count),
        "bucket_prefix": args.bucket_prefix,
        "worker_list": parse_worker_list(args.worker_list),
        "worker_thr": int(args.worker_threads),
        "ttl_sz_cache": parse_size(args.cachemem_size),
        "ttl_prepare_sz": calc_prepare_size(
            parse_size_range(args.object_size),
            int(args.stage_time),
            int(args.iop_limit),
        ),
        "tasks": tasks,
    }

    handle = SlorControl(root_config)
    handle.exec()
