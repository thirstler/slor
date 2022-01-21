import sys, os
import argparse
from multiprocessing.connection import Client
from multiprocessing import Process
from slor_c import *
from shared import *
from driver import _slor_driver, _driver_t, SlorDriver
from workload import *
import json

###############################################################################
## Configuration generation tasks
##
def calc_prepare_size(sizerange, runtime, iops):
    if len(sizerange) > 1:
        avgsz = (sizerange[0] + sizerange[1]) / 2
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


def parse_driver_list(stringval):
    hostlist = []
    for hostport in stringval.split(","):
        if ":" in hostport:
            host = hostport.split(":")[0]
            port = int(hostport.split(":")[1])
        else:
            host = hostport
            port = int(DEFAULT_DRIVER_PORT)
        hostlist.append({"host": host, "port": port})
    return hostlist


def generate_tasks(args):

    loads = list(args.loads.split(","))
    #print(loads)
    mix_prof_obj = {}
    for l in loads:
        if l not in LOAD_TYPES:
            sys.stderr.write('"{0}" is not a load option\n'.format(l))
            sys.exit(1)

    if "mixed" in loads:
        perc = 0
        mix_prof_obj = json.loads(args.mixed_profile)
        for l in MIXED_LOAD_TYPES:
            if l in mix_prof_obj:
                perc += int(mix_prof_obj[l])
        if perc != 100:
            sys.stderr.write("your mixed load profile values don't equal 100\n")
            sys.exit(1)

    # Always happens:
    loads.insert(0, "init")

    # Create a readmap and add prep stage if needed
    if any(x in loads for x in  ['read', 'mixed', 'head', 'delete', 'tag']):
        loads.insert(1, "readmap")
        loads.insert(2, "prepare")


    return {"loadorder": loads, "mixed_profile": mix_prof_obj}


def run():
    parser = argparse.ArgumentParser(
        description="Slor (S3 Load Ruler) is a distributed load generation and benchmarking tool for S3 storage"
    )
    parser.add_argument("controller")  # Make argparse happy
    parser.add_argument(
        "--verbose", action="store_true", default=False, help="verbose output"
    )
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
        help='verify HTTPS certs, defaults to "true"; set to "false" or a path to a CA bundle (bundle needs to be present on all driver hosts',
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
        "--iop-limit",
        default=DEFAULT_UPPER_IOP_LIMIT,
        help="maximum expected IOP/s value that you can expect to hit given the workload; used with --stage-time to calculate the number of objects to preprare",
    )
    parser.add_argument(
        "--prepare-objects",
        default=None,
        help="directly specify the number of objects to prepare (overrides calculation with --iop-limit) ",
    )
    parser.add_argument(
        "--key-length",
        default=DEFAULT_KEY_LENGTH,
        help="key length(s) to use, can be single number or range (e.g. 10,50) - defaults to {0}".format(
            DEFAULT_KEY_LENGTH
        ),
    )
    parser.add_argument(
        "--object-size",
        default=DEFAULT_OBJECT_SIZE,
        help="object size to use; accepts values with common suffixes (1MB, 1MiB) and ranges (1KB-12MiB) - defaults to {0}".format(
            DEFAULT_OBJECT_SIZE
        ),
    )
    parser.add_argument(
        "--workload-file",
        default=None,
        help="specify a workload file in YAML format, ignores most options and executes workload as defined in the file"
    )
    parser.add_argument(
        "--driver-list",
        default="",
        help="comma-delimited list of driver hosts running \"slor driver\" processes (in host:port format); 9256 is assumed if port is excluded",
    )
    parser.add_argument(
        "--processes-per-driver",
        default=DEFAULT_SESSION_COUNT,
        help="number of simultaneous processes per driver host; drivers * processes_per_driver will equal total processes (defaults to {0})".format(
            DEFAULT_SESSION_COUNT
        ),
    )
    parser.add_argument(
        "--cachemem-size",
        default="0",
        help="total amount of memory available in the target cluster for page cache and read caches; post-prepare stage will write this value to overwhelm caches for a cold read stage",
    )
    parser.add_argument(
        "--loads",
        default=DEFAULT_TESTS,
        help="specify the loads you want to run; any (or all) of read, write, delete, head, mixed",
    )
    parser.add_argument(
        "--sleep",
        default=DEFAULT_SLEEP_TIME,
        help="sleeptime between workloads",
    )
    parser.add_argument(
        "--mixed-profile",
        default=DEFAULT_MIXED_PROFILE,
        help="profile of mixed load percentages in JASON format, eg: '{0}'".format(
            DEFAULT_MIXED_PROFILE
        ),
    )
    parser.add_argument(
        "--delimiter-config",
        default=DEFAULT_DELIMITER_CONFIG,
        help="comma-delimited count, depth and length of delimited prefixes (see README)"
    )
    parser.add_argument(
        "--bucket-count",
        default=DEFAULT_BUCKET_COUNT,
        help="number of buckets to distribute over, defaults to '{0}'".format(
            DEFAULT_BUCKET_COUNT
        ),
    )
    args = parser.parse_args()

    if args.workload_file:
        pass
        #root_config = parse_workload(args.workload_file)
        #print(root_config)
        #exit()
    else:
        # if no cmd line args, get from profile, then env (in that order)
        if not args.access_key and not args.secret_key:
            args.access_key, args.secret_key = get_keys(args.profile)

        # Must be AWS if no endpoint is given, to keep boto3 easy we should
        # construct the AWS endpoint explicitly.
        if args.endpoint == "":
            args.endpoint = "https://s3.{0}.amazonaws.com".format(args.region)

        key_sz = args.key_length.split("-")
        if len(key_sz) == 1:
            key_sz = (int(key_sz[0]), int(key_sz[0]), int(key_sz[0]))
        else:
            key_sz = (int(key_sz[0]),
                    int(key_sz[1]),
                    int( ( int(key_sz[0]) + int(key_sz[1]) )/2 ))

        tasks = generate_tasks(args)

        # Start a driver here if there isn't one specified
        driver = None
        if args.driver_list == "":
            args.driver_list = os.uname().nodename
            print("no driver address specified, starting one here")
            driver = Process(target=_slor_driver, args=(args.driver_list, DEFAULT_DRIVER_PORT, True))
            driver.start()
            time.sleep(2)

        if args.prepare_objects != None:
            ttl_prepare_sz = parse_size(args.prepare_objects) * parse_size_range(args.object_size)[2]
        else:
            ttl_prepare_sz = calc_prepare_size(
                parse_size_range(args.object_size),
                int(args.stage_time),
                int(args.iop_limit),
            )
        root_config = {
            "name": args.name,
            "verbose": args.verbose,
            "access_key": args.access_key,
            "secret_key": args.secret_key,
            "endpoint": args.endpoint,
            "verify": args.verify,
            "region": args.region,
            "key_sz": key_sz,
            "sz_range": parse_size_range(args.object_size),
            "run_time": int(args.stage_time),
            "bucket_count": int(args.bucket_count),
            "bucket_prefix": args.bucket_prefix,
            "driver_list": parse_driver_list(args.driver_list),
            "sleeptime": float(args.sleep),
            "driver_proc": int(args.processes_per_driver),
            "ttl_sz_cache": parse_size(args.cachemem_size),
            "iop_limit": int(args.iop_limit),
            "ttl_prepare_sz": ttl_prepare_sz,
            "tasks": tasks,
            "mixed_profile": json.loads(args.mixed_profile)
        }

    handle = SlorControl(root_config)
    handle.exec()
    
    # Join the driver if we started one
    if driver != None:
        driver.join()
