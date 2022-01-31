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
DEFAULT_UPPER_IOP_LIMIT = "0"
DEFAULT_TESTS = "read,write,head,mixed,delete"
DEFAULT_MIXED_PROFILE = '{"read": 60, "write": 25, "delete": 5, "head": 10 }'
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
Usage slor.py [controller|driver|analysis] [options]

Slor is a distributed load generation and benchmarking tool. Please see
README.md for more information.

"""

# Low-level config (changing may or may not break things)
LOG_TO_CONSOLE = True
DRIVER_SOCKET_TIMEOUT = 300  # seconds
FORCE_VERSION_MATCH = True
DRIVER_REPORT_TIMER = 5  # seconds
LOAD_TYPES = ("prepare", "init", "read", "write", "delete", "head", "mixed", "blowout", "cleanup", "tag_read", "tag_write", "sleep")
PROGRESS_BY_COUNT = ("init", "prepare", "blowout")
PROGRESS_BY_TIME = ("read", "write", "mixed", "tag_read", "tag_write", "head", "delete", "tag_read", "tag_write", "sleep")
UNKNOWN_PROGRESS = ("cleanup",)
MIXED_LOAD_TYPES = ("read", "write", "head", "delete", "tag_read", "tag_write", "reread", "overwrite")
OBJECT_PREFIX_LOC = "keys"
PREPARE_RETRIES = 5
SHOW_STATS_RATE = 1
STATS_DB_DIR = "/dev/shm"
TERM_WIDTH_MAX=104
WRITE_STAGE_BYTEPOOL_SZ = 16777216
WINDOWS_DB_TMP = "C:/Windows/Temp/"
POSIX_DB_TMP = "/tmp/"

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    MAGENTA = '\033[35m'
    CYAN = '\033[36m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    GRAY = '\033[38;5;243m'

###############################################################################
## Globally shared routines
##
def parse_size(stringval: str) -> float:

   #if float(stringval)


    """Parse human input for size values"""
    pwr = 10
    sipwr = 3

    for s in ["KiB", "MiB", "GiB", "TiB", "PiB", "EiB"]:
        if stringval[-3:] == s:
            return float(stringval[0:-3]) * (2 ** pwr)
        pwr += 10

    for s in ["KB", "MB", "GB", "TB", "PB", "EB"]:
        # Deal with single-letter suffixes and two letter (e.g. "MB" and "M")
        if stringval[-1:] == s[0]: 
            return float(stringval[0:-1]) * (10 ** sipwr)
        if stringval[-2:] == s:
            return float(stringval[0:-2]) * (10 ** sipwr)
        sipwr += 3

    return float(stringval)


def human_readable(value, val_format="SI", print_units="bytes", precision=2):
    
    if val_format == "SI":
        sipwr = 18
        
        if print_units == "ops":
            units = ["E", "P", "T", "G", "M", "K", ""]
        else:
            units = [" EB", " PB", " TB", " GB", " MB", " KB", " B"]

        for s in units:
            if value > 10 ** sipwr or (10 ** sipwr) == 1:
                return "{0:.{2}f}{1}".format(value / (10 ** sipwr), s, precision)
            sipwr -= 3
    else:
        # Or else what? WHAT?
        pass


def basic_sysinfo():
    """
    Never would have used psutil to start with if I knew is sucked this hard
    """

    # Older versions of psutil don't have getloadavg(), pft.
    sysload = []
    try:
        sysload = psutil.getloadavg()
    except AttributeError:
        if os.name == "posix":
            with open('/proc/loadavg') as f:
                for i, item in  enumerate(f.readlines()[0].split(" ")):
                    if i > 2: break
                    sysload.append(float(item))
        else:
            sysload = [0.0, 0.0, 0.0]
    except:
        # Fuck it
        sysload = [0.0, 0.0, 0.0]

    # Not every platform has this either
    try:
        cpu_freq = psutil.cpu_freq()
    except AttributeError:
        cpu_freq = None

    # Not every platform has this either
    try:
        sensors = psutil.sensors_temperatures(),
    except AttributeError:
        sensors = None


    return {
        "slor_version": SLOR_VERSION,
        "uname": platform.uname(),
        "sysload": sysload,
        "cpu_perc": psutil.cpu_percent(1),
        "vm": psutil.virtual_memory(),
        "cpus": psutil.cpu_count(),
        "cpu_freq": cpu_freq,
        "net": psutil.net_io_counters(pernic=True),
        "sensors": sensors,
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

def gen_key(key_desc=(40, 40), prefix="", inc=None, chars=string.digits + string.ascii_uppercase) -> str:
    if type(key_desc) == int:
        key_desc = (key_desc, key_desc)
    key =  "{0}{1}".format(
        prefix,
        "".join(
            random.choice(chars)
            for _ in range(0, key_desc[0] if key_desc[0] == key_desc[1] else random.randrange(key_desc[0], key_desc[1]) )
        ),
    )
    if inc:
        inc=str(inc)
        chars = len(inc)
        key = (key[:-chars] + inc) if chars < len(key) else inc
        
    return key

def sample_structure(operations):
    sample = {
        "start": 0,          # start of sample
        "end": 0,            # end of sample
        "st": {},            # dict of operation types
        "perc": 0,           # precent complete (for iterable benchmarks - blowout, prepare)
        "ios": 0             # I/Os global to load
    }
    for t in operations:
        sample["st"][t] = {
            "resp": [],      # list of response times (seconds)
            "bytes": 0,      # total bytes present in sample
            "bytes/s": 0,
            "ios": 0,        # total io operations present in sample
            "ios/s": 0,
            "failures": 0,   # total number of failures present in the sample
            "iotime": 0,     # time spend in io (seconds)
        }
    return sample
