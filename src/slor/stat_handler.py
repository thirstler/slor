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
        
        if sample_addr not in self.standing_sample:
            self.standing_sample[sample_addr] = perfSample(from_json=data["value"])
        else:
            self.standing_sample[sample_addr].from_json(data["value"])
            

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

        screen.clear_data()
        title_text = "Running stage: {}".format(opclass_from_label(self.stage))
        if int(self.stage[self.stage.find(":")+1:]) > 0:
            title_text += "({})".format(self.stage[self.stage.find(":")+1:])
        screen.set_title(title_text)

        if self.stage == "init":
            if final:
                screen.data_win.addstr("done.\n")
            return

        stat_sample = self.mk_merged_sample()

        if stat_sample.global_io_count < 1:
            return

        # Check if all workers are reporting
        if self.ttl_procs == len(self.standing_sample):
            self.all_checked_in = True

        # If we're in a cleaup stage override all this shit and do CYAN    
        if self.stage == "cleanup":
            color = bcolors.CYAN

        # Multiple operations in the sample (mixed)
        if len(stat_sample.operations) > 1:

            # Workloads with time limit (any benchmark workload)
            if self.stage_class in PROGRESS_BY_TIME:
                perc = 0
                # Nothing reported in yet set values
                if stat_sample.sample_seq == 0:
                    self.progress_start_time = time.time()
                else:
                    perc = (time.time() - self.progress_start_time) / self.duration

                screen.set_progress(perc, final=final)

            screen.data_win.addstr("\n Elapsed: {}\n".format(self.elapsed_time()))
            screen.show_bench_table(stat_sample, stat_sample.operations)

        
            if show_plots:
                io_time_ttl = []
                for op in stat_sample.operations:
                    io_time_ttl += stat_sample.get_metric("iotime", op)

                iotime_ms = list(map(lambda n: n*1000, io_time_ttl))
                if self.all_checked_in:
                    self.peak_optime =  max(iotime_ms) if max(iotime_ms) > self.peak_optime else self.peak_optime
                    self.min_optime = min(iotime_ms) if min(iotime_ms) < self.min_optime else self.min_optime
                screen.data_win.addstr("\n "+"─"*90+"\n")
                screen.data_win.addstr(" Operation time distribution (99% percentile):\n\n")
                hist_values = histogram_data(iotime_ms, 72, trim=0.99, max_x=self.graph_x_max)
                this_max_x = max(hist_values, key=lambda x:x['val'])['val']
                this_max_y = max(hist_values, key=lambda x:x['count'])['count']
                if this_max_x > self.graph_x_max: self.graph_x_max = this_max_x
                if this_max_y > self.graph_y_max: self.graph_y_max = this_max_y
                histogram_graph_curses(hist_values, screen.data_win, height=8, max_y=self.graph_y_max, units="ms")
                screen.data_win.addstr("\n"+average_plot(iotime_ms, 72, trim=0.99, min_x=self.min_optime, max_x=self.graph_x_max))
                screen.data_win.addstr("\n           ┍ min:{:.2f}ms ┿ median:{:.2f}ms ╋ mean:{:.2f}ms > peak:{:.2f}ms".format(
                    self.min_optime if self.min_optime != MAX_MIN_MS else -1,
                    statistics.median(iotime_ms), statistics.mean(iotime_ms), self.peak_optime), curses.A_DIM)
                screen.data_win.noutrefresh()

        # Discrete operation
        else:
            
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

            screen.data_win.addstr("\n Elapsed: {}\n".format(self.elapsed_time()))
            screen.show_bench_table(stat_sample, stat_sample.operations)
                
            if show_plots:
                iotime = stat_sample.get_metric("iotime", self.operations[0])
                iotime_ms = list(map(lambda n: n*1000, iotime))
                if self.all_checked_in:
                    self.peak_optime = max(iotime_ms) if max(iotime_ms) > self.peak_optime else self.peak_optime
                    self.min_optime = min(iotime_ms) if min(iotime_ms) < self.min_optime else self.min_optime
                screen.data_win.addstr("\n "+"─"*90+"\n")
                screen.data_win.addstr(" Operation time distribution (99% percentile):\n\n")
                hist_values = histogram_data(iotime_ms, 72, trim=0.99, max_x=self.graph_x_max)
                this_max_x = max(hist_values, key=lambda x:x['val'])['val']
                this_max_y = max(hist_values, key=lambda x:x['count'])['count']
                if this_max_x > self.graph_x_max: self.graph_x_max = this_max_x
                if this_max_y > self.graph_y_max: self.graph_y_max = this_max_y
                histogram_graph_curses(hist_values, screen.data_win, height=8, max_y=self.graph_y_max, units="ms")
                screen.data_win.addstr("\n"+average_plot(iotime_ms, 72, trim=0.99, min_x=self.min_optime, max_x=self.graph_x_max))
                screen.data_win.addstr("\n           ┍ min:{:.2f}ms ┿ median:{:.2f}ms ╋ mean:{:.2f}ms > peak:{:.2f}ms".format(
                    self.min_optime if self.min_optime != MAX_MIN_MS else -1,
                    statistics.median(iotime_ms), statistics.mean(iotime_ms), self.peak_optime), curses.A_DIM)
                screen.data_win.noutrefresh()

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


    def disp_bytes_sec(self, bytes_sec):
        return "{}/s".format(human_readable(bytes_sec))


    def disp_ops_sec(self, ops_sec):
        h_rate = human_readable(ops_sec, print_units="ops")
        return "{}/s".format(h_rate)


    def disp_resp(self, resp_avg, precision=2):
        return "{0:.{1}f} ms".format(resp_avg * 1000, precision)


    def disp_failure_count(self, count=0, width=12):
        return "{}/s".format(human_readable(count, print_units="ops"))
