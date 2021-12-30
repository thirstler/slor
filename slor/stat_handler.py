import json
import sqlite3
import time
import json
from shared import *


class rtStatViewer:

    progress_type = None
    progress_timeout = None
    progress_time_len = None
    progress_time_start = None
    progress_count = None
    stage = None
    window = 0
    data = {}
    last_seen = 0
    config = None

    def __init__(self, config):
        self.config = config
    
    def clear(self):
        self.data.clear()
        # Create data data structure to hold current stats
        for stage in self.config["tasks"]["loadorder"]:
            self.data[stage] = {}
            for worker in self.config["worker_node_names"]:
                self.data[stage][worker] = {}
                for t in range(0, self.config["worker_thr"]):
                    self.data[stage][worker][t] = {}

    def set_stage(self, stage):
        self.clear()
        self.stage = stage

    def store(self, message):
        self.data[message["stage"]][message["w_id"]][message["t_id"]].update(message["value"])
        #print(message["value"])

    def set_progress_time(self, time_length):
        self.progress_type = "time"
        self.progress_time_start = time.time()
        self.progress_time_len = time_length
        self.progress_timeout = self.progress_time_start + time_length

    def set_progress_count(self, count_length):
        self.progress_type = "count"
        self.progress_count = count_length


    def show(self, disply_rate, now=time.time(), final=False):
        
        if (now - self.last_seen) < disply_rate:
            if final == False: return
        
        ops_sec = 0
        count = 0
        avg_resp = 0
        perc_complete = 0
        failures = 0
        num = 0 # input to progress bar
        of = 0  # input to progress bar
        for worker in self.data[self.stage]:
            for process in self.data[self.stage][worker]:
                me = self.data[self.stage][worker][process]
                if "count" in me: count += me["count"]
                if "iops" in me: ops_sec += me["iops"]
                if "failures" in me: failures += me["failures"]
                resp = 0
                if "resp" in me:
                    for r in me["resp"]:
                        resp += r
                    avg_resp = resp/len(me["resp"])


        if self.progress_type == "time":
            num = time.time() - self.progress_time_start
            of = self.progress_time_len
            if of > self.progress_timeout:
                of == self.progress_timeout

        elif self.progress_type == "count":
            num = count
            of = self.progress_count

        else: pass # you should never get here

        if num > of: of = num
        self.progress(num, of, ops_sec, resp*1000, title=self.stage, final=final)
        self.last_seen = now
    
    def progress(self, num, of,  ops, resp, title="", final=False, bar_width=35):
        """Ultra simple progress bar"""
        file=sys.stdout

        if len(title) > PROGRESS_TITLE_LEN:
            title = title[:PROGRESS_TITLE_LEN]
        else:
            title = "{0}:{1}".format(title, " " * (PROGRESS_TITLE_LEN - len(title)))
        
        width = math.floor( (num / of) * bar_width)
        perc_done = math.floor((num / of) * 100)

        file.write(
            "\r{0:<10}|{1}{2}|{3:>4}% [{4:>9.2f} op/s] [{4:>9.2f} ms]".format(title, "|" * width, "-" * (bar_width - width), perc_done, ops, resp)
        )
        if final:
            file.write("\n")
        file.flush()