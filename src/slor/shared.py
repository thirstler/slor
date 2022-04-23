import json
import platform
import configparser
import os, sys
import random
import string
from tkinter.font import ITALIC
from unicodedata import numeric

SLOR_VERSION = 0.4

# Defaults
DEFAULT_PROFILE_DEF = ""
DEFAULT_ENDPOINT = None
DEFAULT_REGION = "us-east-1"
DEFAULT_BUCKET_PREFIX = "slor-"
DEFAULT_BENCH_LEN = "300"
DEFAULT_OBJECT_SIZE = "1MB"
DEFAULT_DRIVER_PORT = "9256"
DEFAULT_DRIVER_LIST = "localhost:{0}".format(DEFAULT_DRIVER_PORT)
DEFAULT_SESSION_COUNT = "10"
DEFAULT_UPPER_IOP_LIMIT = "1000"
DEFAULT_TESTS = "read,write,head,mixed,delete,cleanup"
DEFAULT_MIXED_PROFILE = '[{"read": 5, "write": 2, "delete": 2, "head": 3}]'
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
Slor is a distributed load generation and benchmarking tool. Please see
README.md for more information.

Usage slor.py [controller|driver|analysis] [options]

  --version             display version and exit
"""

# Low-level config (changing may or may not break things)
LOG_TO_CONSOLE = True
DRIVER_SOCKET_TIMEOUT = 300  # seconds
FORCE_VERSION_MATCH = True
DRIVER_REPORT_TIMER = 5  # seconds
STATS_QUANTA = 5  # seconds (should probably be the same as DRIVER_REPORT_TIMER)
LOAD_TYPES = (
    "prepare",
    "init",
    "read",
    "write",
    "delete",
    "head",
    "mixed",
    "blowout",
    "cleanup",
    "tag_read",
    "tag_write",
    "sleep",
)
PROGRESS_BY_COUNT = ("init", "prepare", "blowout")
PROGRESS_BY_TIME = (
    "read",
    "write",
    "mixed",
    "tag_read",
    "tag_write",
    "head",
    "delete",
    "tag_read",
    "tag_write",
    "sleep",
)
UNKNOWN_PROGRESS = ("cleanup",)
MIXED_LOAD_TYPES = (
    "read",
    "write",
    "head",
    "delete",
    "tag_read",
    "tag_write",
    "reread",
    "overwrite",
)
OBJECT_PREFIX_LOC = "keys"
PREPARE_RETRIES = 5
SHOW_STATS_RATE = 1
STATS_DB_DIR = "/dev/shm"
TERM_WIDTH_MAX = 104
WRITE_STAGE_BYTEPOOL_SZ = 16777216
WINDOWS_DB_TMP = "C:/Windows/Temp/"
POSIX_DB_TMP = "/tmp/"
MINIMUM_MPU_CHUNK_SIZE = 5000000

##
# Soem color short-hand
class bcolors:
    HEADER = "\033[95m"
    OKBLUE = "\033[94m"
    OKCYAN = "\033[96m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"
    GRAY = "\033[38;5;243m"
    ITALIC = '\033[3m'

class sizeRange:

    # Defaults
    low:int = None
    high:int = None
    avg:float = None

    def __init__(self, low:int=None, high:int=None, range_arg:str=None):

        # Create range values from argument input
        if range_arg:
            items = range_arg.split("-")
            if len(items) == 1:
                low = parse_size(items[0])
                high = low
            elif len(items) == 2:
                low = parse_size(items[0])
                high = parse_size(items[1])

        # I'm sure there's a more clever way to do this:
        if low and high:
            self.avg = (low+high)/2
            self.low = low
            self.high = high
        elif low:
            self.avg = low
            self.high = low
            self.low = low
        elif high:
            self.avg = high
            self.high = high
            self.low = high
    
    def getVal(self) -> int:
        """return random int in range"""
        if self.low == self.high:
            return round(self.low)
        else:
            return round(self.low + ((self.high-self.low) * random.random()))

    def serialize(self):
        # Need to avoid pickling in the future.
        return {"low": self.low, "high": self.high, "avg": self.high}



BANNER = "\n<<{0}SLoR{1}>> (ver. {2})\n".format(
    bcolors.BOLD, bcolors.ENDC, SLOR_VERSION
)
###############################################################################
## Globally shared routines
##
def parse_size(stringval: str) -> int:

    # if float(stringval)
    if stringval == None:
        return None

    """Parse human input for size values"""
    pwr = 10
    sipwr = 3

    for s in ["KiB", "MiB", "GiB", "TiB", "PiB", "EiB"]:
        if stringval[-3:] == s:
            return round(float(stringval[0:-3]) * (2**pwr))
        pwr += 10

    for s in ["KB", "MB", "GB", "TB", "PB", "EB"]:
        # Deal with single-letter suffixes and two letter (e.g. "MB" and "M")
        if stringval[-1:] == s[0]:
            return round(float(stringval[0:-1]) * (10**sipwr))
        if stringval[-2:] == s:
            return round(float(stringval[0:-2]) * (10**sipwr))
        sipwr += 3

    return int(stringval)


def human_readable(value, val_format="SI", print_units="bytes", precision=2):
    if not value:
        return 0
    if val_format == "SI":
        sipwr = 18

        if print_units == "ops":
            units = ["E", "P", "T", "G", "M", "K", ""]
        else:
            units = [" EB", " PB", " TB", " GB", " MB", " KB", " B"]

        for s in units:
            if value > 10 ** sipwr or (10**sipwr) == 1:
                return "{0:.{2}f}{1}".format(value / (10**sipwr), s, precision)
            sipwr -= 3
    else:
        # Or else what? WHAT?
        pass


def basic_sysinfo():
    """
    This is pretty much useless. Was using psutil for all kinds of nifty info
    but it wasn't worth it. Module is a pain to install with pip (requires
    gcc), it's not installed by default and not worth the effort.
    """

    # Older versions of psutil don't have getloadavg(), pft.
    if os.name == "posix":
        load1, load5, load15 = os.getloadavg()
        sysload = [load1, load5, load15]
    else:
        # Not checked on windows and I don't want the psutil module 
        sysload = [0.0, 0.0, 0.0]

    return {
        "slor_version": SLOR_VERSION,
        "uname": platform.uname(),
        "sysload": sysload
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


def gen_key(
    key_desc=(40, 40), prefix="", inc=None, chars=string.digits + string.ascii_uppercase
) -> str:
    if type(key_desc) == int:
        key_desc = (key_desc, key_desc)
    key = "{0}{1}".format(
        prefix,
        "".join(
            random.choice(chars)
            for _ in range(
                0,
                key_desc[0]
                if key_desc[0] == key_desc[1]
                else random.randrange(key_desc[0], key_desc[1]),
            )
        ),
    )
    if inc:
        inc = str(inc)
        chars = len(inc)
        key = (key[:-chars] + inc) if chars < len(key) else inc

    return key


def opclass_from_label(label):
    return label[: label.find(":")] if ":" in label else label


def top_box():
    print("\u250C{0}".format("\u2500" * (os.get_terminal_size().columns - 1)))


def bottom_box():
    print("\u2514{0}".format("\u2500" * (os.get_terminal_size().columns - 1)))


def box_text(text):
    text_lines = text.split("\n")
    top_box()
    for line in text_lines:
        print("\u2502 " + line)
    bottom_box()

def mixed_ratio_perc(mixed_json):
    rttl = 0
    percentages = {}
    for op in mixed_json:
        rttl += mixed_json[op]
    for op in mixed_json:
        percentages[op] = mixed_json[op]/rttl
    return percentages
    

