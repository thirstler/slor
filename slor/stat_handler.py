import time, os
from shared import *


class rtStatViewer:

    progress_type = None
    progress_timeout = None
    progress_time_len = None
    progress_time_start = None
    progress_count = None
    object_count_shadow = 0 # used to track I/Os for time-based progress
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
            for driver in self.config["driver_node_names"]:
                self.data[stage][driver] = {}
                for t in range(0, self.config["driver_proc"]):
                    self.data[stage][driver][t] = {}

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
        self.progress_time_start = time.time()

    def arbitrary_progress(self, count, of, ops=0, title="", final=False):
        self.progress(count, of, ops, 0, 0, 0, title=title, final=final)
        self.object_count_shadow = count

    def show(self, disply_rate, now=time.time(), final=False):
        
        if self.stage == "init":
            self.object_count_shadow = 0
            if final:
                self.progress(100, 100,  0, 0, 0, 0, title=self.stage, final=True)
            else:
                sys.stdout.write("\r{}:".format(self.stage))
        else:
            self._show(disply_rate, now=now, final=final)

    def _show(self, disply_rate, now=time.time(), final=False):
        
        if (now - self.last_seen) < disply_rate:
            if final == False: return
        
        ops_sec = 0
        count = 0
        self.object_count_shadow = 0
        avg_resp = 0
        failures = 0
        bandwidth = 0
        num = 0 # input to progress bar
        of = 0  # input to progress bar
        for driver in self.data[self.stage]:
            for process in self.data[self.stage][driver]:

                # Shorthand
                me = self.data[self.stage][driver][process]

                if "count" in me:
                    count += me["count"]
                    self.object_count_shadow = count

                if "failures" in me:
                    failures += me["failures"]

                if final:
                    if "benchmark_iops" in me: ops_sec += me["benchmark_iops"]
                    if "benchmark_bandwidth" in me: bandwidth += me["benchmark_bandwidth"]
                else:    
                    if "iops" in me: ops_sec += me["iops"]
                    if "bandwidth" in me: bandwidth += me["bandwidth"]

                resp = 0
                if "resp" in me and len(me["resp"]) > 0:
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

        if num > of: of = num
        self.progress(num, of, ops_sec, bandwidth, avg_resp*1000, failures, title=self.stage, final=final)
        self.last_seen = now
    
    def progress(self, num, of,  ops, bandwidth, resp, failures, title="", final=False):
        """
        This has gotten really trashy, please rewrite
        """
        file=sys.stdout
        bar_width = os.get_terminal_size().columns - 79
        if bar_width > 25:
            bar_width = 25
        
        try:
            width = math.ceil( (num / of) * bar_width)
            perc_done = math.ceil((num / of) * 100)
        except ZeroDivisionError:
            width = math.ceil(bar_width)
            perc_done = 0

        failtxt = "       -"
        if num > 0:
            try:
                perc_f = failures/self.object_count_shadow
            except:
                perc_f = 0
            if perc_f >= 0.0001:
                failtxt = "{:.4f}%".format(perc_f*100)
            else:
                failtxt = "{0}/{1}".format(
                    human_readable(failures, print_units="ops", precision=0),
                    human_readable(self.object_count_shadow, print_units="ops", precision=0))
        
        elapsed = time.time() - self.progress_time_start
        hours, remainder = divmod(elapsed, 3600)
        minutes, seconds = divmod(remainder, 60)
        elapsed = '{:02}:{:02}:{:02}'.format(int(hours), int(minutes), int(seconds))
        prog = "\r{0:<8}{1}{2} {3:>3}% {4} {5} {6} [{7:>8}] [{8:>8}]".format(
                    title,
                    "/" * width,
                    #u"\u2588" * width,
                    "-" * (bar_width - width),
                    perc_done,
                    "[           -]" if ops == 0 else "[{:>7} op/s]".format(human_readable(ops, print_units="ops")),
                    "[           -]" if bandwidth == 0 else "[{:>10}/s]".format(human_readable(bandwidth)),
                    "[         -]" if resp == 0 else "[{:>10.2f}]".format(resp),
                    elapsed,
                    failtxt)
        file.write(prog)
        
        if final:
            file.write("\n")
        file.flush()
