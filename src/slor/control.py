from slor.screen import ConsoleToSmall
from slor.slor_c import *
from slor.shared import *
from slor.output import *
from slor.driver import _slor_driver
from slor.workload import *
import argparse
from multiprocessing.connection import Client
from multiprocessing import Process
from curses import wrapper

def start_driver():

    driver_list = "127.0.0.1"
    print("no driver address specified, starting one here")
    driver = Process(target=_slor_driver, args=(driver_list, DEFAULT_DRIVER_PORT, True))
    driver.start()
    time.sleep(2)

    return driver


def input_checks(args):
    """
    Before any parsing, perform sanity checks
    """
    keepgoing = True # fatal error occurred if true
    warnings = ""    # hold warnings, show them
    errors = ""      # error messages

    try:
        mixed_profiles = json.loads(args.mixed_profiles)
    except json.decoder.JSONDecodeError as e:
        keepgoing = False
        errors += "check mixed-profiles JSON string, it's busted: {}\n".format(e)

    if args.force:
        return True

    # Confirm before saving a readmap file that will have missing objects
    # on the storage system due to delete workload.
    if args.save_readmap and (
        any(args.loads.find(x) for x in ("cleanup", "delete"))
        or (args.loads.find("mixed") and args.mixed_profiles.find("delete"))
    ):
        sys.stdout.write(
            "It looks like you're saving the readmap but have delete operations in your\n" +
            "workload. If you try to use this readmap for subsequent loads there will be\n" +
            "objects missing. ")
        yn = input("You sure you mean this? (y/n): ")
        if yn[0].upper == "N":
            keepgoing = False # Error

    # You specified a "blowout" stage but no cacheme-size?
    if "blowout" in args.loads.split(",") and args.cachemem_size == "0":
        warnings += "blowout specified but no data amount defined (--cachemem-size), skipping blowout.\n"
        keepgoing = True # Warning

    # MPU side too small for soe reason?
    if args.mpu_size and parse_size(args.mpu_size) <= MINIMUM_MPU_CHUNK_SIZE:
        errors += "MPU part size too small, must be greather than {}\n".format(
                human_readable(MINIMUM_MPU_CHUNK_SIZE)
            )
        keepgoing = False # Error

    # Get-range sanity check
    if args.get_range and (parse_size(args.get_range.split("-")[-1]) > parse_size(args.object_size.split("-")[0])):
        errors += "cannot perform get-range operations on objects smaller than the range size\n"
        keepgoing = False # Error

    # read workloads after deletes?
    del_occurred = 0
    mixed_index = 0
    for w in args.loads.split(","):
        if w == "delete":
            del_occurred += 1
        if w == "mixed":
            if "delete" in mixed_profiles[mixed_index]:
                del_occurred += 1
            mixed_index += 1

        if (del_occurred > 0 and w in ("head", "read", "reread", "tag_read")) or del_occurred > 1:
            errors += "you've specified a \"read\" workload when data might be missing from a previous \"delete\" workload\n"
            keepgoing = False # Error


    if errors:
        box_text("{}Argument error(s){}:\n".format(bcolors.BOLD, bcolors.ENDC) + errors)
    elif warnings:
        box_text("{}Argument warning(s){}:\n".format(bcolors.BOLD, bcolors.ENDC) + warnings)

    return keepgoing


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
        "--op-ceiling",
        default=DEFAULT_UPPER_IOP_LIMIT,
        help="read operations/s ceiling you expect to achieve; used with --stage-time to determine the amount of data to prepare",
    )
    parser.add_argument(
        "--prepare-objects",
        default=None,
        help="directly specify the amount of objects to prepare (count or size; e.g. 1.2TB, 50000K) - overrides calculation with --op-ceiling",
    )
    parser.add_argument(
        "--bucket-prefix",
        default=DEFAULT_BUCKET_PREFIX,
        help='prefix to use when creating buckets (defaults to "{0}")'.format(
            DEFAULT_BUCKET_PREFIX
        ),
    )
    parser.add_argument(
        "--key-length",
        default=DEFAULT_KEY_LENGTH,
        help="key length(s) to use, can be single number or range (e.g. 10-50) - defaults to {0}".format(
            DEFAULT_KEY_LENGTH
        ),
    )
    parser.add_argument(
        "--key-prefix",
        default="",
        help="prefix for all written keys (prepared or for write tests)",
    )
    parser.add_argument(
        "--object-size",
        default=DEFAULT_OBJECT_SIZE,
        help="object size to use; accepts values with common suffixes (1MB, 1MiB) and ranges (1KB-12MiB) - defaults to {0}".format(
            DEFAULT_OBJECT_SIZE
        ),
    )
    parser.add_argument(
        "--random-from-pool",
        action="store_true",
        default=False,
        help="use a pool of random data when writing rather than generate it on-the-fly - uses less CPU, but storage systems that dedupe will know",
    )
    parser.add_argument(
        "--compressible",
        default=0,
        help="make random data compressible by N percent",
    )
    parser.add_argument(
        "--get-range",
        default=None,
        help="specify a size or size range (e.g.: 1024-9172, or 712 or 15K) to get from prepared objects, noting that it cannot be larger than the prepared objects"
    )
    parser.add_argument(
        "--mpu-size", default=None, help="write objects as MPUs using this chunk size"
    )
    parser.add_argument(
        "--versioning", action="store_true", help="use versioned buckets, include versioned read, re-read and delete requests when possible during mixed workloads (see README)"
    )
    parser.add_argument(
        "--driver-list",
        default="",
        help='comma-delimited list of driver hosts running "slor driver" processes (in host:port format); 9256 is assumed if port is excluded',
    )
    parser.add_argument(
        "--processes-per-driver",
        default=DEFAULT_SESSION_COUNT,
        help="number of simultaneous processes per driver host; drivers * processes_per_driver will equal total processes (defaults to {0})".format(
            DEFAULT_SESSION_COUNT
        ),
    )
    parser.add_argument(
        "--prepare-procs-per-driver",
        default="-1",
        help="number of processes per driver to run during the prepare stage, defaults to processes-per-driver value".format(),
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
        "--cleanup",
        action="store_true",
        default=False,
        help="remove objects when finished with workload",
    )
    parser.add_argument(
        "--remove-buckets",
        action="store_true",
        default=False,
        help="delete buckets when finished with workload (enables --cleanup option)",
    )
    parser.add_argument(
        "--bucket-count",
        default=DEFAULT_BUCKET_COUNT,
        help="number of buckets to distribute over, defaults to '{0}'".format(
            DEFAULT_BUCKET_COUNT
        ),
    )
    parser.add_argument(
        "--use-existing-buckets",
        action="store_true",
        default=False,
        help="force the use of existing buckets; WARNING: destruction of ALL DATA IN THE BUCKET(S) WILL HAPPEN if you specify this with --cleanup or --remove-buckets"
    )
    parser.add_argument(
        "--sleep",
        default=DEFAULT_SLEEP_TIME,
        help="sleeptime between workloads"
    )
    parser.add_argument(
        "--mixed-profiles",
        default=DEFAULT_MIXED_PROFILE,
        help="list of profiles for mixed loads in JSON format (see README); each operation is given a share: e.g. '{0}'".format(
            DEFAULT_MIXED_PROFILE
        ),
    )
    parser.add_argument(
        "--save-readmap",
        default=False,
        help="save readmap (location of prepared objects) for use in later runs",
    )
    parser.add_argument(
        "--use-readmap",
        default=False,
        help="use readmap - this will obviate a prepare step and assume objects in the readmap exist",
    )
    parser.add_argument(
        "--no-db",
        action="store_true",
        default=False,
        help="do not save workload data to database - appropriate for long-running tests",
    )
    parser.add_argument(
        "--no-plot",
        action="store_true",
        default=False,
        help="do not do any data visualization in the console",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="force 'yes' answer to any requests for input (e.g. 'are you sure?')",
    )
    parser.add_argument(
        "--version",
        action="store_true",
        default=False,
        help="display version and exit",
    )
    args = parser.parse_args()

    if args.version:
        print("SLoR version: {}".format(SLOR_VERSION))
        sys.exit(0)

    print(BANNER)
    
    if not input_checks(args):
        sys.exit(1)

    if args.driver_list == "":
        driver = start_driver()
        args.driver_list = "127.0.0.1"

    root_config = classic_workload(args)
    
    try:
        handle = SlorControl(root_config)
        wrapper(handle.exec)
    except ConsoleToSmall:
        sys.stderr.write('console too small, need {}x{}\n'.format(TERM_ROW_MIN,TERM_COL_MIN))
    except PeerCheckFailure:
        sys.stderr.write("driver check failed, make sure they're running and reachable\n")

    try:
        driver.terminate()
        driver.join()
    except:
        pass
