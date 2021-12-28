import json
import sqlite3
import time
import json
from shared import *


class textStats:
    def __init__(self, stage="unknown"):
        pass

    def trigger(self, final=False):
        pass


class timingStats:
    """
    Used for temporal progress (how much longer?)
    """

    start_time = 0
    end_time = 0
    nownow = 0
    stage = None
    last_print = 0
    peak = False

    def __init__(self, timing, start_time, run_len, stage="unknown"):
        self.start_time = start_time
        self.end_time = start_time + run_len
        self.timing = timing
        self.run_len = run_len
        self.stage = stage

    def trigger(self, final=False):
        self.nownow = time.time()
        if (self.nownow - self.last_print) > self.timing:
            self.print_frame()
            self.last_print = self.nownow

    def print_frame(self):
        if self.peak:
            return
        file = sys.stdout
        final = False
        s_done = math.ceil(self.nownow - self.start_time)
        if s_done >= self.run_len:
            s_done = self.run_len
            self.peak = True

        progress(s_done, self.run_len, title=(self.stage), file=file, final=self.peak)


class dbStats:
    """
    Stat handler. Information for real-time output to the console is queried
    from the stats database rather than from in-memory state.
    """

    db_conn = None
    db_cursor = None
    last_print = 0
    timing = 0
    frame = None
    frame_start = None
    last_count = 0
    maplen = 0
    stage = None

    def __init__(self, db_file, timing, readmap_len, stage):
        self.frame = {}
        self.timing = timing
        self.maplen = readmap_len
        self.stage = stage
        try:
            self.db_conn = sqlite3.connect(db_file)
            self.db_cursor = self.db_conn.cursor()
        except:
            print("error accessing db file ({0}) no stats will print".format(db_file))

    def trigger(self, final=False):
        nownow = time.time()
        if ((nownow - self.last_print) > self.timing) or final == True:
            self.process()
            self.last_print = nownow

    def process(self):
        query = "SELECT name FROM sqlite_master WHERE type='table'"
        then = int((time.time() - self.timing) * 1000)
        for table in self.db_cursor.execute(query).fetchall():
            query = "SELECT stage, data, ts FROM {0} WHERE ts > {1} AND stage='{2}' ORDER BY ts".format(
                table[0], then, self.stage
            )
            for stat in self.db_cursor.execute(query):
                if stat[0] == "prepare":
                    self.proc_prep(stat[1])

        self.print_frame()

    def proc_prep(self, data_json):
        """
        This seems deeply fucked to me but I don't have any better ideas.
        """
        data = json.loads(data_json)
        if data["stage"] not in self.frame:
            self.frame[data["stage"]] = {}
        if data["w_id"] not in self.frame[data["stage"]]:
            self.frame[data["stage"]][data["w_id"]] = {}
        if data["t_id"] not in self.frame[data["stage"]][data["w_id"]]:
            self.frame[data["stage"]][data["w_id"]][data["t_id"]] = {}

        if "time" not in self.frame[data["stage"]][data["w_id"]][data["t_id"]]:
            self.frame[data["stage"]][data["w_id"]][data["t_id"]] = {
                "time": data["time"],
                "count": data["value"]["count"],
            }
        else:
            # There will be repeats in the time window queried, simply store
            # over the existing data when the timestamp is bigger, latest one
            # wins.
            if (
                self.frame[data["stage"]][data["w_id"]][data["t_id"]]["time"]
                < data["time"]
            ):
                self.frame[data["stage"]][data["w_id"]][data["t_id"]] = {
                    "time": data["time"],
                    "count": data["value"]["count"],
                }

    def print_frame(self):
        file = sys.stdout
        for stage in self.frame:
            if stage == "prepare":
                total = 0
                for wrkr in self.frame["prepare"]:
                    for thr in self.frame["prepare"][wrkr]:
                        total += self.frame["prepare"][wrkr][thr]["count"]
                rate = float(total - self.last_count) / SHOW_STATS_EVERY
                progress(total, self.maplen, title=stage, file=file, info=" [{0:.1f} op/s]".format(rate))
                self.last_count = total
