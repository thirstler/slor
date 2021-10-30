import platform
import psutil


SLOR_VERSION = 0.1

# Defaults
DEFAULT_PROFILE_DEF = "default"
DEFAULT_ENDPOINT = "https://s3.us-east-1.amazonaws.com"
DEFAULT_REGION = "us-east-1"
DEFAULT_BUCKET_PREFIX = "slor-"
DEFAULT_BENCH_LEN = "300"
DEFAULT_OBJECT_SIZE = "1MB"
DEFAULT_WORKER_PORT = "9256"
DEFAULT_WORKER_LIST = "localhost:{0}".format(DEFAULT_WORKER_PORT)
DEFAULT_SESSION_COUNT = "1"
DEFAULT_UPPER_IOP_LIMIT = "10000"
DEFAULT_TESTS = "read,write,delete,head,mixed"
DEFAULT_MIX_PROFILE = '{"read": 60, "write": 25, "delete": 5, "head": 10 }'
DEFAULT_PREPARE_SIZE = "8M"
DEFAULT_BUCKET_COUNT = 1

# Root help message
ROOT_HELP = """
Usage slor.py [controller|worker] [options]

Slor is a distributed load generation and benchmarking tool. Please see
README.md for more information.

"""

# Low-level config
LOG_TO_CONSOLE = True
WORKER_SOCKET_TIMEOUT = 300
FORCE_VERSION_MATCH = True
WORKER_REPORT_TIMER = 5
WORKER_ROUTINE_TYPES = ("prepare", "read", "readwrite", "write", "mixed", "overrun")

###############################################################################
###############################################################################
## Shared routines
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
