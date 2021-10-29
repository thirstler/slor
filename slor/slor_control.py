import sys
import argparse
from multiprocessing.connection import Client
import socket
import time
import json
from shared import *

class SlorControl():

    config = None
    conn = []

    def __init__(self, root_config):
        self.config = root_config

    def log(self, message):
        if LOG_TO_CONSOLE:
            sys.stdout.write("{0}\n".format(message))
            sys.stdout.flush()
        else:
            pass # fuck, I don't know...
    
    def print_msg(self, message):
        if not "message" in message: return False
        print("{0}: {1}".format("unknown" if not "source" in message else message["source"], message["message"]))

    def connect_to_workers(self):
        ret_val = True
        for hostport in self.config["worker_list"]:
            self.log("=== worker system '{0}:{1}' ===".format(hostport["host"], hostport["port"]))
            try:
                self.conn.append(Client( (hostport["host"], hostport["port"])))
                self.conn[-1].send({"command": "sysinfo"})
                resp = self.conn[-1].recv()
                self.log(self.check_worker_info(resp))

            except ConnectionRefusedError:
                sys.stderr.write("  connection refused on {0}:{1}, exiting\n".format(hostport["host"], hostport["port"]))
                ret_val = False
            except socket.timeout:
                sys.stderr.write("  timeout connecting to {0}:{1}, exiting\n".format(hostport["host"], hostport["port"]))
                ret_val = False
            except OSError as e:
                sys.stderr.write("  {2} for {0}:{1}, exiting\n".format(hostport["host"], hostport["port"], e))
                ret_val = False
            except Exception as e:
                sys.stderr.write("  unexpected exception ({2}) connecting to {0}:{1}, exiting\n".format(hostport["host"], hostport["port"], e))
                ret_val = False

        return ret_val


    def check_worker_info(self, data):
        print("  Hostname: {0}".format(data["uname"].node))
        print("  OS: {0} release {1}".format(data["uname"].system, data["uname"].release))
        if data["slor_version"] != SLOR_VERSION:
            print("  \033[1;31mwarning\033[0m: version mismatch; controller is {0} worker is {1}".format(SLOR_VERSION, data["slor_version"]))
        if data["sysload"][0] >= 1:
            print("  \033[1;31mwarning\033[0m: worker 1m sysload is > 1 ({0})".format(data["sysload"][0]))
        for iface in data["net"]:
            if (data["net"][iface].errin + data["net"][iface].errout + data["net"][iface].dropin +data["net"][iface].dropout) > 0:
                print("  \033[1;31mwarning\033[0m: iface {0} has dropped packets or errors".format(iface))

    def exec(self):

        self.log("\nstarting up...\n")
        if not self.connect_to_workers():
            self.log("worker(s) failed check(s), I'm out")
            sys.exit(1)
        else:
            self.log("workers look good...")

        for stage in self.config["tasks"]["loadorder"]:
            self.exec_stage(stage)

        for c in self.conn:
            c.close()

    def exec_stage(self, stage):
        workloads = []
        # Create the workloads
        for target in self.config["worker_list"]:
            stage_cfg = self.mk_stage(target, stage)
            if stage_cfg: workloads.append(stage_cfg)
            else:
                return

        # Send to hosts
        for i in range(0, len(workloads)):
            self.conn[i].send({"command": "workload", "config": workloads[i]})

        donestack = len(workloads)
        
        while True:

            for i in range(0, len(workloads)):
                while self.conn[i].poll():
                    try:
                        mesg = self.conn[i].recv()

                        if "status" in mesg and mesg["status"] == "done":
                            donestack -= 1
                        if "message" in mesg:
                            self.print_msg(mesg)

                    except EOFError:
                        pass
            
            if donestack == 0:
                print("all threads done?")
                break

            time.sleep(0.1)


    def mk_stage(self, target, stage):

        config = False
        if stage == "prepare":
            config = {
                "host": target["host"],
                "port": target["port"],
                "type": "prepare",
                "threads": self.config["worker_thr"],
                "access_key": self.config["access_key"],
                "secret_key": self.config["secret_key"],
                "endpoint": self.config["endpoint"],
                "ca": self.config["ca"],
                "region": self.config["region"],
                "run_time": self.config["run_time"],
                "sz_range": self.config["sz_range"],
                "prepare_sz": int(self.config["ttl_prepare_sz"] / len(self.config["worker_list"]))
            }
        return config

###############################################################################
###############################################################################
## Conifg generation tasks
##
def calc_prepare_size(sizerange, runtime, iops):
    if len(sizerange) > 1:
        avgsz = ((sizerange[1] - sizerange[0])/2) + sizerange[0]
    else:
        avgsz = sizerange[0]

    return(avgsz * iops * runtime)


def parse_size_range(stringval):
    
    if not "-" in stringval:
        sz = parse_size(stringval)
        return( (sz,sz,sz) )
    
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

    choose_any = ("read" "write" "delete" "head" "mixed")
    loads = tuple(args.loads.split(","))
    mix_prof_obj = {}
    for l in loads:
        if l not in choose_any:
            sys.stderr.write("\"{0}\" is not a load option\n".format(l))
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
    actions = ()
    if len(loads) == 1 and  "write" in loads: pass
    else: actions += ("prepare",)
    if "write" in loads: actions += ("write",)
    if "read" in loads: actions += ("blowout", "read")
    if "head" in loads: actions += ("head",)
    if "mixed" in loads: actions += ("blowout", "mixed")
    if "delete" in loads: actions += ("delete",) # debateable, might want cache overwritten as well?

    return {"loadorder": actions, "mixed_profile": mix_prof_obj}


def run():
    parser = argparse.ArgumentParser(
        description="Slor (S3 Load Ruler) is a distributed load generation and benchmarking tool for S3 storage"
    )
    parser.add_argument("controller") # Make argparse happy
    parser.add_argument(
        "--bucket-prefix",
        default=DEFAULT_BUCKET_PREFIX,
        help='prefix to use when creating buckets (defaults to "{0}")'.format(
            DEFAULT_BUCKET_PREFIX
        ),
    )
    parser.add_argument("--profile", default=DEFAULT_PROFILE_DEF)
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    parser.add_argument("--ca-bundle", default=False)
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--access-key", required=True)
    parser.add_argument("--secret-key", required=True)
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
        )
    )
    parser.add_argument(
        "--cachemem-size",
        default="0",
        help="total amount of memory available in the target cluster for page cache and read caches; post-prepare stage will write this value to overwhelm caches for a cold read stage"
    )
    parser.add_argument(
        "--iop-limit",
        default=DEFAULT_UPPER_IOP_LIMIT,
        help="maximum expected IOP/s value that you can expect to hit given the workload; needed to determine the size of the prepare data given the load run-time"
    )
    parser.add_argument(
        "--loads",
        default=DEFAULT_TESTS,
        help="specify the loads you want to run; any (or all) of read, write, delete, head, mixed"
    )
    parser.add_argument(
        "--mix-profile",
        default=DEFAULT_MIX_PROFILE,
        help="profile of mixed load percentages in JASON format, eg: '{0}'".format(DEFAULT_MIX_PROFILE)
    )
    parser.add_argument(
        "--bucket-count",
        default=DEFAULT_BUCKET_COUNT,
        help="number of buckets to distribute over, defaults to '{0}'".format(DEFAULT_BUCKET_COUNT)
    )
    args = parser.parse_args()

    tasks = generate_tasks(args)

    root_config = {
        "access_key": args.access_key,
        "secret_key": args.secret_key,
        "endpoint": args.endpoint,
        "ca": args.ca_bundle,
        "region": args.region,
        "sz_range": parse_size_range(args.object_size),
        "run_time": int(args.stage_time),
        "bucket_count": args.bucket_count,
        "worker_list": parse_worker_list(args.worker_list),
        "worker_thr": int(args.worker_threads),
        "ttl_sz_cache": parse_size(args.cachemem_size),
        "ttl_prepare_sz": calc_prepare_size(parse_size_range(args.object_size), int(args.stage_time), int(args.iop_limit)),
        "tasks": tasks
    }

    handle = SlorControl(root_config)
    handle.exec()
