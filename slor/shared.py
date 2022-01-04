import platform
import psutil
import configparser
import os, sys
import math
import random
import string

SLOR_VERSION = 0.1

# Defaults
DEFAULT_PROFILE_DEF = ""
DEFAULT_ENDPOINT = ""
DEFAULT_REGION = "us-east-1"
DEFAULT_BUCKET_PREFIX = "slor-"
DEFAULT_BENCH_LEN = "300"
DEFAULT_OBJECT_SIZE = "1MB"
DEFAULT_DRIVER_PORT = "9256"
DEFAULT_DRIVER_LIST = "localhost:{0}".format(DEFAULT_DRIVER_PORT)
DEFAULT_SESSION_COUNT = "1"
DEFAULT_UPPER_IOP_LIMIT = "10000"
DEFAULT_TESTS = "read,write,head,mixed,delete"
DEFAULT_MIX_PROFILE = '{"read": 60, "write": 25, "delete": 5, "head": 10 }'
DEFAULT_PREPARE_SIZE = "8M"
DEFAULT_BUCKET_COUNT = 1
DEFAULT_WRITE_PREFIX = "write/"
DEFAULT_KEY_LENGTH = "40"
DEFAULT_READMAP_PREFIX = "read/"
DEFAULT_CACHE_OVERRUN_OBJ = 8388608
DEFAULT_CACHE_OVERRUN_PREFIX = "overrun/"
DEFAULT_SLEEP_TIME = 30

# Root help message
ROOT_HELP = """
Usage slor.py [controller|driver] [options]

Slor is a distributed load generation and benchmarking tool. Please see
README.md for more information.

"""

# Low-level config (changing may or may not break things)
LOG_TO_CONSOLE = True
DRIVER_SOCKET_TIMEOUT = 300  # seconds
FORCE_VERSION_MATCH = True
DRIVER_REPORT_TIMER = 5  # seconds
LOAD_TYPES = ("prepare", "init", "read", "write", "delete", "head", "mixed", "blowout", "cleanup", "tag", "sleep")
PROGRESS_BY_COUNT = ("init", "prepare", "blowout", "cleanup")
PROGRESS_BY_TIME = ("read", "write", "mixed", "blowout", "tag")
OBJECT_PREFIX_LOC = "keys"
PREPARE_RETRIES = 5
SHOW_STATS_RATE = 1 # seconds
STATS_DB_DIR = "/dev/shm"
TERM_WIDTH_MAX=104
WRITE_STAGE_BYTEPOOL_SZ = 16777216

###############################################################################
## Globally shared routines
##
def parse_size(stringval: str) -> float:
    """Parse human input for size values"""
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


def human_readable(value, format="SI", print_units="bytes", precision=2):
    sipwr = 18
    
    if print_units == "ops":
        units = ["E", "P", "T", "G", "M", "K", ""]
    else:
        units = [" EB", " PB", " TB", " GB", " MB", " KB", " B"]

    for s in units:
        if value > 10 ** sipwr or (10 ** sipwr) == 1:
            return "{0:.{2}f}{1}".format(value / (10 ** sipwr), s, precision)
        sipwr -= 3

    return "??" # you shouldn't get here


def basic_sysinfo():

    # Older versions of psutil don't have getloadavg(), pft.
    sysload = []
    try:
        sysload = psutil.getloadavg()
    except AttributeError:
        with open('/proc/loadavg') as f:
            for i, item in  enumerate(f.readlines()[0].split(" ")):
                if i > 2: break
                sysload.append(float(item))
    except:
        # Fuck it
        sysload = [0.0, 0.0, 0.0]

    return {
        "slor_version": SLOR_VERSION,
        "uname": platform.uname(),
        "sysload": sysload,
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

def gen_key(key_desc=(40, 40), prefix="", chars=string.digits + string.ascii_lowercase) -> str:
    if type(key_desc) == int:
        key_desc = (key_desc, key_desc)
    return "{0}{1}".format(
        prefix,
        "".join(
            random.choice(chars)
            for _ in range(0, key_desc[0] if key_desc[0] == key_desc[1]  else random.randrange(key_desc[0], key_desc[1]) )
        ),
    )
