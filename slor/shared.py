import platform
import psutil
import configparser
import os, sys

SLOR_VERSION = 0.1

# Defaults
DEFAULT_PROFILE_DEF = ""
DEFAULT_ENDPOINT = ""
DEFAULT_REGION = "us-east-1"
DEFAULT_BUCKET_PREFIX = "slor-"
DEFAULT_BENCH_LEN = "300"
DEFAULT_OBJECT_SIZE = "1MB"
DEFAULT_WORKER_PORT = "9256"
DEFAULT_WORKER_LIST = "localhost:{0}".format(DEFAULT_WORKER_PORT)
DEFAULT_SESSION_COUNT = "1"
DEFAULT_UPPER_IOP_LIMIT = "10000"
DEFAULT_TESTS = "read,write,head,mixed,delete"
DEFAULT_MIX_PROFILE = '{"read": 60, "write": 25, "delete": 5, "head": 10 }'
DEFAULT_PREPARE_SIZE = "8M"
DEFAULT_BUCKET_COUNT = 1
DEFAULT_WRITE_PREFIX = "write"

# Root help message
ROOT_HELP = """
Usage slor.py [controller|worker] [options]

Slor is a distributed load generation and benchmarking tool. Please see
README.md for more information.

"""

# Low-level config
LOG_TO_CONSOLE = True
WORKER_SOCKET_TIMEOUT = 300  # seconds
FORCE_VERSION_MATCH = True
WORKER_REPORT_TIMER = 5  # seconds
WORKER_ROUTINE_TYPES = ("prepare", "read", "readwrite", "write", "mixed", "overrun")
OBJECT_PREFIX_LOC = "keys"
PREPARE_RETRIES = 5
SHOW_STATS_EVERY = 5
STATS_DB_DIR = "/dev/shm"

###############################################################################
###############################################################################
## Globally shared routines
##
def parse_size(stringval):
    pwr = 10
    sipwr = 3

    for s in ["KiB", "MiB", "GiB", "TiB", "PiB", "EiB"]:
        if stringval[-3:] == s:
            return float(stringval[0:-3]) * (2 ** pwr)
        pwr += 10

    for s in ["KB", "MB", "GB", "TB", "PB", "EB"]:
        if stringval[-1:] == s[0]:
            return float(stringval[0:-1]) * (10 ** sipwr)
        if stringval[-2:] == s:
            return float(stringval[0:-2]) * (10 ** sipwr)
        sipwr += 3

    return float(stringval)


def human_readable(value, format="SI"):
    sipwr = 18

    for s in ["EB", "PB", "TB", "GB", "MB", "KB"]:
        if value > 10 ** sipwr:
            return "{0:.2f} {1} ".format(value / (10 ** sipwr), s)
        sipwr -= 3

    return "{0} b ".format(value)


def basic_sysinfo():
    return {
        "slor_version": SLOR_VERSION,
        "uname": platform.uname(),
        "sysload": psutil.getloadavg(),
        "cpu_perc": psutil.cpu_percent(1),
        "vm": psutil.virtual_memory(),
        "cpus": psutil.cpu_count(),
        "cpu_freq": psutil.cpu_freq(),
        "net": psutil.net_io_counters(pernic=True),
        "sensors": psutil.sensors_temperatures(),
    }


def get_keys(profile):
    """
    Just open the ~/.aws/credentials file and get the creds, this is
    easier than digging around in boto3
    """
    config = configparser.ConfigParser()
    config.read("{0}/.aws/credentials".format(os.environ["HOME"]))

    if profile not in config or (
        "aws_access_key_id" not in config[profile]
        or "aws_secret_access_key" not in config[profile]
    ):
        # Environment? Grab it here 
        access_key = os.environ.get("AWS_ACCESS_KEY_ID")
        secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        if access_key is None or secret_key is None:
            sys.stderr.write("Nope: no access/secret keys found\n")
            return ("", "")
    else:
        access_key = config[profile]["aws_access_key_id"]
        secret_key = config[profile]["aws_secret_access_key"]
        

    return (access_key, secret_key)
