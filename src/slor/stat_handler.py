from slor.shared import *
from slor.sample import perfSample
from slor.output import *
from slor.plots import *
import time
import math
import statistics
import math

class statHandler:

    config = None
    standing_sample = {}
    sample_freshness = {}
    operations = None
    last_show = 0
    stage = None
    stage_class = None
    workload_index = None
    all_checked_in = None
    count_target = 0
    global_io_counter = 0
    progress_start_time = 0
    stage_start_time = 0
    last_rm_count = 0
    last_rm_time = 0
    stat_rotation = float(0)
    stat_types = ("throughput", "bandwidth", "response")
    last_stage = None
    reported_in = ()
    ttl_procs = 0
    duration = 0
    progress_indx = 0
    headers = True
    graph_x_max = 0
    graph_y_max = 0
    peak_optime = 0
    min_optime = MAX_MIN_MS
    data_written = 0
    data_read = 0

    def __del__(self):
        self.fh.close()
        self.standing_sample.clear()

    def __init__(self, config, stage, duration, workload_index=0):
        self.config = config
        self.operations = ()
        self.stage = stage
        self.stage_class = opclass_from_label(stage)
        self.stage_start_time = time.time()
        self.progress_start_time = self.stage_start_time
        self.duration = duration
        self.reread = 0
        self.operation_hist = {}
        self.fh = open("/tmp/log.txt", "a")
        self.workload_index = workload_index
        self.all_checked_in = False
        self.io_hist = []
        self.peak_reset = time.time()
        if self.stage_class == "mixed":
            for s in config["mixed_profile"]:
                self.operations += (s,)
        else:
            if any(self.stage_class == x for x in ("prepare", "blowout")):
                self.operations = ("write",)
            else:
                self.operations = (self.stage_class,)

        # How many processes in this job are we expecting?
        self.ttl_procs = len(self.config["driver_list"]) * self.config["driver_proc"]

        # Overrideable config (kill me now)
        if self.stage_class in self.config["tasks"]["config_supplements"]:

            # Process count override
            if "processes" in self.config["tasks"]["config_supplements"][self.stage_class]:
                self.ttl_procs = self.config["tasks"]["config_supplements"][self.stage_class]["processes"]

    def set_count_target(self, count):
        self.count_target = count

    def update_standing_sample(self, data):

        sample_addr = "{}:{}".format(data["w_id"], data["t_id"])
        nownow = time.time()
        
        if sample_addr not in self.standing_sample:
            self.standing_sample[sample_addr] = perfSample(from_json=data["value"])
            self.sample_freshness[sample_addr] = time.time()
        else:
            self.standing_sample[sample_addr].from_json(data["value"])
            self.sample_freshness[sample_addr] = time.time()

    def rotate_mixed_stat_func(self, final):
        if final:
            self.stat_rotation = 0
        if self.stat_types[int(self.stat_rotation)] == "throughput":
            disp_func = self.disp_ops_sec
        elif self.stat_types[int(self.stat_rotation)] == "bandwidth":
            disp_func = self.disp_bytes_sec
        elif self.stat_types[int(self.stat_rotation)] == "response":
            disp_func = self.disp_resp
        self.stat_rotation += 0.20
        if self.stat_rotation >= 3:
            self.stat_rotation = 0
        return disp_func

    # one-off routines to make things a little more readable
    def expire_standing_samples(self, ref_time=time.time()):
        delete_us = []
        for sid in self.standing_sample:
            if self.standing_sample[sid].ttl == 0:
                continue
            if self.standing_sample[sid].ttl > ref_time:
                delete_us.append(sid)

        for sid in reversed(delete_us):
            del self.standing_sample[sid]


    def get_standing_window_average(self):
        window_avg = 0
        for sid in self.standing_sample:
            window_avg += (
                self.standing_sample[sid].window_end
                - self.standing_sample[sid].window_start
            )
        return window_avg


    def mk_merged_sample(self):
        now = time.time()

        # Deal with stale samples
        diediedie = []
        for sid in self.sample_freshness:
            if self.sample_freshness[sid] < (now - (DRIVER_REPORT_TIMER*2)):
                diediedie.append(sid)

        for sid in diediedie:
            try:
                del self.standing_sample[sid]
                del self.sample_freshness[sid]
            except:
                pass

        if self.stage_class in PROGRESS_BY_COUNT:
            target = self.count_target
        else:
            target = None
        stat_sample = perfSample(count_target=target)
        for sid in self.standing_sample:
            stat_sample.merge(self.standing_sample[sid])
            
        window_avg = self.get_standing_window_average()

        if len(self.standing_sample) > 0:
            window_avg /= len(self.standing_sample)
            stat_sample.start(start_time=(now - window_avg))
            stat_sample.stop(stop_time=now)

        return stat_sample


    def show(self, screen, final=False):

        show_plots = True 
        if self.stage_class in UNKNOWN_PROGRESS:
            show_plots = False
        if self.config["no_plot"] == True:
            show_plots = False


        # Determine if it's time to show the next sample or not
        now = time.time()
        if (self.last_show + SHOW_STATS_RATE) >= now and final != True:
            return

        screen.data_win.clear()
        screen.title_win.clear()
        screen.title_win.addstr(BANNER + " ")
        screen.data_win.addstr(" stages: ")
        for n, s in enumerate(self.config["tasks"]["loadorder"]):
            if s == self.stage:
                screen.data_win.addstr("{}".format(opclass_from_label(s)), curses.A_BOLD)
            else: 
                screen.data_win.addstr("{}".format(opclass_from_label(s)), curses.A_DIM)
            if s != self.config["tasks"]["loadorder"][-1]:
                screen.data_win.addstr(" -> ")
        screen.data_win.addstr("\n")
        screen.data_win.noutrefresh()


        stat_sample = self.mk_merged_sample()

        if stat_sample.global_io_count < 1:
            return

        # Check if all workers are reporting
        if len(self.standing_sample) >= self.ttl_procs:
            self.all_checked_in = True
        else:
            self.all_checked_in = False
        
        for op in stat_sample.operations:
            if op == "write":
                self.data_written += stat_sample.get_rate("bytes", op)
            if op == "read":
                self.data_read += stat_sample.get_rate("bytes", op)

        screen.title_win.addstr("reporting processes: {}/{}, elapsed time: {}".format(
            len(self.standing_sample), self.ttl_procs, self.elapsed_benchmark()))
        screen.title_win.noutrefresh()

        screen.data_win.addstr(" data written: {}, read: {}, stage time: {}\n".format(
            human_readable(self.data_written), human_readable(self.data_read), self.elapsed_time()))

        cmd = screen.footer_win.getch()
        if cmd == 104:
            self.graph_y_max = 0
            self.graph_x_max = 0
            
        # Workloads with time limit (any benchmark workload)
        if self.stage_class in PROGRESS_BY_TIME:
            perc = 0
            # Nothing to report,set  dummy values
            if stat_sample.global_io_count == 0:
                self.progress_start_time = time.time()
            else:
                perc = (time.time() - self.progress_start_time) / (
                    self.duration + (DRIVER_REPORT_TIMER * 2)
                )
            if perc > 1:
                perc = 1
            screen.set_progress(perc, final=final)

        # Work-to-finish workloads (prepare, blowout)
        elif self.stage_class in PROGRESS_BY_COUNT:
            screen.set_progress(stat_sample.percent_complete(), final=final)
        
        # Unknown terminus (cleanup)
        elif self.stage_class in UNKNOWN_PROGRESS:
            screen.set_progress(None, final=final)


        screen.show_bench_table(stat_sample, stat_sample.operations)

        if show_plots:
            # So much can go wrong here, best not to mess up a benchmark for it
            try:
                io_time_ttl = []
                self.io_hist.append({"ts": time.time(), "ios": round(stat_sample.get_rate("ios"))})
                for op in stat_sample.operations:
                    io_time_ttl += stat_sample.get_metric("iotime", op)

                iotime_ms = list(map(lambda n: n*1000, io_time_ttl))
                plot_color=7
                if self.all_checked_in:
                    self.peak_optime =  max(iotime_ms) if max(iotime_ms) > self.peak_optime else self.peak_optime
                    self.min_optime = min(iotime_ms) if min(iotime_ms) < self.min_optime else self.min_optime
                    plot_color=6
                #screen.data_win.addstr("\n "+"─"*90+"\n")
                
                hist_values = histogram_data(iotime_ms, 72, trim=0.99, max_x=self.graph_x_max)
                this_max_x = max(hist_values, key=lambda x:x['val'])['val']
                this_max_y = max(hist_values, key=lambda x:x['count'])['count']
                if this_max_x > self.graph_x_max: self.graph_x_max = this_max_x
                if this_max_y > self.graph_y_max: self.graph_y_max = this_max_y
                screen.data_win.addstr("\n")
                io_history(self.io_hist, screen.data_win, config=self.config, stage_class=self.stage_class, color=plot_color)
                screen.data_win.addstr("\n")
                histogram_graph_curses(hist_values, screen.data_win, height=8, max_y=self.graph_y_max, units="ms", color=plot_color)
                screen.data_win.addstr("\n"+average_plot(iotime_ms, 72, trim=0.99, min_x=self.min_optime, max_x=self.graph_x_max))
                screen.data_win.noutrefresh()
            except Exception as e:
                screen.data_win.addstr("problem generating plots\n")
            
        screen.refresh()

        del stat_sample

        # print(self.stat_rotation)
        self.last_show = now

    def readmap_progress(self, x, outof, final=False):
        
        retstr = ""

        if (x % 10000) == 0 or final:
            nownow = time.time()
        else:
            return None

        perc = 1 if final else (x / outof)
        try:
            rate = (x - self.last_rm_count) / (nownow - self.last_rm_time)
        except ZeroDivisionError:
            rate = 0

        
        retstr += self.progress(perc, final=final)
        retstr += " {}".format("[{:>7} op/s]".format(human_readable(rate, print_units="ops")))

        self.last_rm_time = nownow
        self.last_rm_count = x

        return retstr


    def dunno(self, width=10, final=False, color="", printme=True):
        blocks = ("▏", "▎","▍", "▌","▋", "▊","▉", "█","▉", "▊","▋", "▌","▍", "▎","▏")
        
        if final:
            characters = "{}{} 100%{}".format(bcolors.GRAY, "\u2588" * width, bcolors.ENDC)

        else:
            characters = ""
            for b in range(0, width):
                characters += blocks[self.progress_indx % len(blocks)]
                self.progress_indx += 1
            characters += " ???%"
        
        if printme:
            sys.stdout.write(characters)
        else:
            return characters

    def progress(self, perc, width=25, final=False, color="", printme=False):
        if final or perc > 1:
            perc = 1
        blocks = ("▏","▎","▍", "▌", "▋", "▊", "▉", "█") # eighth blocks
        fillchar = "\u2588"
        char_w = perc * width
        leading_char = blocks[math.floor((char_w * 8) % 8)]
        
        if final:
            color = bcolors.GRAY
            leading_char = " "
        if self.reread > 0:
            color = bcolors.WARNING

        characters = "{}{}{}{:>3}%".format(
                fillchar * (math.floor(char_w)),
                leading_char,
                " " * (width - math.floor(char_w)),
                math.ceil(perc * 100)
            )
        if printme:
            sys.stdout.write(characters)
        else:
            return characters

    def dev_color(self, value, stddiv):
        if value == 0 or stddiv == 0: return bcolors.GRAY
        color = bcolors.GRAY
        if stddiv/value > 0.128:
            color = bcolors.WARNING
        elif stddiv/value > 0.25:
            color = bcolors.FAIL
        return color


    def elapsed_time(self):
        elapsed = time.time() - self.stage_start_time
        hours, remainder = divmod(elapsed, 3600)
        minutes, seconds = divmod(remainder, 60)
        return "{}".format(
            "{:02}:{:02}:{:02}".format(int(hours), int(minutes), int(seconds))
        )

    def elapsed_benchmark(self):
        try:
            elapsed = time.time() - self.config["benchmark_start"]
            hours, remainder = divmod(elapsed, 3600)
            minutes, seconds = divmod(remainder, 60)
            elapsed_str = "{:02}:{:02}:{:02}".format(int(hours), int(minutes), int(seconds))
        except:
            elapsed_str = "unknown"
        
        return elapsed_str

    def disp_bytes_sec(self, bytes_sec):
        return "{}/s".format(human_readable(bytes_sec))


    def disp_ops_sec(self, ops_sec):
        h_rate = human_readable(ops_sec, print_units="ops")
        return "{}/s".format(h_rate)


    def disp_resp(self, resp_avg, precision=2):
        return "{0:.{1}f} ms".format(resp_avg * 1000, precision)


    def disp_failure_count(self, count=0, width=12):
        return "{}/s".format(human_readable(count, print_units="ops"))
