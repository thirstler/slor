from slor.slor_c import *
from slor.shared import *
from slor.driver import _slor_driver
from slor.workload import *
import argparse
from multiprocessing.connection import Client
from multiprocessing import Process


def start_driver():

    driver_list = "127.0.0.1"
    print("no driver address specified, starting one here")
    driver = Process(target=_slor_driver, args=(driver_list, DEFAULT_DRIVER_PORT, True))
    driver.start()
    time.sleep(2)

    return driver


def input_checks(args):

    keepgoing = True

    if args.force:
        return True

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
            keepgoing = False

    if args.loads.find("blowout") and args.cachemem_size == "0":
        sys.stdout.write(
            "blowout specified but no data amount defined (--cachemem-size), skipping blowout.\n"
        )

    if args.mpu_size and parse_size(args.mpu_size) <= MINIMUM_MPU_CHUNK_SIZE:
        sys.stdout.write(
            "MPU part size too small, must be greather than {}\n".format(
                human_readable(MINIMUM_MPU_CHUNK_SIZE)
            )
        )
        keepgoing = False

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
        "--iop-limit",
        default=DEFAULT_UPPER_IOP_LIMIT,
        help="maximum expected IOP/s value that you can expect to hit given the workload; used with --stage-time to calculate the number of objects to preprare",
    )
    parser.add_argument(
        "--prepare-objects",
        default=None,
        help="directly specify the amount of objects to prepare (count or size; e.g. 1.2TB, 50000K) - overrides calculation with --iop-limit",
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
        help="key length(s) to use, can be single number or range (e.g. 10,50) - defaults to {0}".format(
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
        "--get-range",
        default=None,
        help="specify a size or size range (e.g.: 1024-9172, or 712 or 15K) to get from prepared objects, noting that it cannot be larger than the prepared objects"
    )
    parser.add_argument(
        "--mpu-size", default=None, help="write objects as MPUs using this chunk size"
    )
    # parser.add_argument(
    #    "--workload-file",
    #    default=None,
    #    help="specify a workload file in YAML format, ignores most options and executes workload as defined in the file"
    # )
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
        help="list of profiles of mixed loads in JSON format, eg: '{0}'".format(
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
        help="do not save workload data to database - appropriate for long-running tests",
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

    if not input_checks(args):
        sys.exit(1)

    if args.driver_list == "":
        driver = start_driver()
        args.driver_list = "127.0.0.1"

    root_config = classic_workload(args)

    handle = SlorControl(root_config)
    handle.exec()

    try:
        driver.join()
    except:
        pass
