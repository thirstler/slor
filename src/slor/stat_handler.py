from slor.shared import *
from slor.sample import perfSample
import time
import math
import statistics

class statHandler:

    config = None
    standing_sample = {}
    operations = None
    last_show = 0
    stage = None
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

    def __del__(self):
        self.standing_sample.clear()

    def __init__(self, config, stage, duration):
        self.config = config
        self.operations = ()
        self.stage = stage
        self.stage_start_time = time.time()
        self.progress_start_time = self.stage_start_time
        self.duration = duration
        self.reread = 0
        self.operation_hist = {}

        if self.stage == "mixed":
            for s in config["mixed_profile"]:
                self.operations += (s,)
        else:
            if any(self.stage == x for x in ("prepare", "blowout")):
                self.operations = ("write",)
            else:
                self.operations = (self.stage,)

        # How many processes in this job are we expecting?
        self.ttl_procs = len(self.config["driver_list"]) * self.config["driver_proc"]

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
            disp_func = self.disp_resp_avg
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
        if self.stage in PROGRESS_BY_COUNT:
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


    def print_workload_headers(self, header_items):
        sys.stdout.write("\r")
        sys.stdout.write("\u2502" + " " * 25)
        items = []
        for o in header_items:
            items.append("{}".format(o))
        for i in items:
            sys.stdout.write("{:>15}".format(i))
        sys.stdout.write("    elapsed")
        sys.stdout.write("\n")


    def headers(self, mixed_count):
        """Show headers for the stage we're about to display stats for"""
        if self.stage == "mixed":
            hlist = []
            for x in self.config["mixed_profiles"][mixed_count]:
                hlist.append(x)
            hlist.append("total")
            self.print_workload_headers(hlist)

        elif any(
            self.stage == x
            for x in MIXED_LOAD_TYPES + ("prepare", "blowout", "cleanup")
        ):
            self.print_workload_headers(
                ("throughput", "bandwidth", "resp ms", "failures")
            )


    def show(self, final=False):

        # signal if we can start using figures for calculating a final average
        # for dispaly.
        calc_avg = False 

        # Determine if it's time to show the next sample or not
        now = time.time()
        if (self.last_show + SHOW_STATS_RATE) >= now and final != True:
            return

        if self.stage == "init" and final:
            sys.stdout.write("\r\u2502 init:    done")
            if final:
                sys.stdout.write("\n")
            sys.stdout.flush()
            return
        elif self.stage == "init":
            sys.stdout.write("\r\u2502 init:")
            if final:
                sys.stdout.write("\n")
            sys.stdout.flush()
            return

        sys.stdout.write("\r")

        stat_sample = self.mk_merged_sample()
        # self.expire_standing_samples(ref_time=now)

        if stat_sample.global_io_count < 1:
            sys.stdout.write("\r\u2502 [ waiting for processes... ]")
            sys.stdout.flush()
            return

        # change color to indicated if all processes have checked on or not
        if final:
            color = ""
        elif len(self.standing_sample) == 0:
            color = bcolors.FAIL
        elif self.ttl_procs > len(self.standing_sample):
            color = bcolors.OKGREEN
        elif self.ttl_procs == len(self.standing_sample):
            color = bcolors.OKBLUE

            # Now that all processes are reporting, start using figures for
            # calculating a final averages.
            calc_avg = True

        else:  # should never happen
            color = bcolors.FAIL

        sys.stdout.write(
            "\u2502 {}{:<9}{}".format(color, self.stage + ":", bcolors.ENDC)
        )

        # Multipal operations in the sample (mixed)
        if len(stat_sample.operations) > 1:

            # Workloads with time limit (any benchmark workload)
            if self.stage in PROGRESS_BY_TIME:
                perc = 0
                # Nothing reported in yet set values
                if stat_sample.sample_seq == 0:
                    self.progress_start_time = time.time()
                else:
                    perc = (time.time() - self.progress_start_time) / self.duration

                if perc > 1:
                    perc = 1  # Just in case

                self.progress(perc, final=final)

            # Work-to-finish workloads (won't happen w/mixed workload)
            elif self.stage in PROGRESS_BY_COUNT:
                self.progress(stat_sample.percent_complete(), final=final)

            if not "global_rate" in self.operation_hist:
                self.operation_hist["global_rate"] = []
            rate_s = stat_sample.get_workload_io_rate()
            if calc_avg and not final:
                self.operation_hist["global_rate"].append(rate_s)
            
            # show each operation ops/s
            for o in self.operations:
                resp_a = stat_sample.get_resp_avg(o)

                if calc_avg and not final:
                    if not o in self.operation_hist:
                        self.operation_hist[o] = []
                    self.operation_hist[o].append(resp_a)
                
                if final:
                    sys.stdout.write(
                        " {}".format(self.disp_resp_avg(statistics.mean(self.operation_hist[o])))
                    )
                else:
                    sys.stdout.write(
                        " {}".format(self.disp_resp_avg(resp_a))
                    )

            # show total ops/s
            if final:
                sys.stdout.write(
                    " {}".format(self.disp_ops_sec(statistics.mean(self.operation_hist["global_rate"])))
                )
            else:
                sys.stdout.write(
                    " {}".format(self.disp_ops_sec(rate_s))
                )

            sys.stdout.write(" {}".format(self.elapsed_time()))
            
            if final:
                sys.stdout.write("\n")
                sys.stdout.write("\u2502" + "          {0}stability (CV):".format(bcolors.ITALIC+bcolors.GRAY, bcolors.ENDC))
                for o in self.operations:
                    resp_a = statistics.mean(self.operation_hist[o])
                    resp_sd = statistics.stdev(self.operation_hist[o])
                    respdev_col = self.dev_color(resp_a, resp_sd)
                    sys.stdout.write("{0}{1}{2:>13.2f}%{3} ".format(
                        bcolors.ITALIC, respdev_col, (resp_sd/resp_a)*100, bcolors.ENDC)
                    )
                sys.stdout.write("\n\u2502")

        # Discrete operation
        else:

            if self.stage in PROGRESS_BY_TIME:
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
                self.progress(perc, final=final)

            # Work-to-finish workloads (prepare, blowout)
            elif self.stage in PROGRESS_BY_COUNT:
                self.progress(stat_sample.percent_complete(), final=final)

            # Unknown terminus (cleanup)
            elif self.stage in UNKNOWN_PROGRESS:
                self.dunno(final=final)

            for op in stat_sample.operations: # there's only one operation if we're here

                rate_s = stat_sample.get_rate("ios", op)
                bytes_s = stat_sample.get_rate("bytes", op)
                resp_a = stat_sample.get_resp_avg(op)
                failures = stat_sample.get_metric("failures", op)

                if calc_avg and not final:
                    if not op in self.operation_hist:
                        self.operation_hist[op] = {
                            "ios": [],
                            "bytes": [],
                            "resp": []
                        }
                    self.operation_hist[op]["ios"].append(rate_s)
                    self.operation_hist[op]["bytes"].append(bytes_s)
                    self.operation_hist[op]["resp"].append(resp_a)
                elif final:
                    try:
                        rate_s = sum(self.operation_hist[op]["ios"])/len(self.operation_hist[op]["ios"])
                        bytes_s = sum(self.operation_hist[op]["bytes"])/len(self.operation_hist[op]["bytes"])
                        resp_a = sum(self.operation_hist[op]["resp"])/len(self.operation_hist[op]["resp"])
                    except KeyError:
                        # If a stage stops immediately, then there won't be an
                        # operations history. Give up.
                        pass

                sys.stdout.write( 
                    " {} {} {} {} {}".format(
                        self.disp_ops_sec(rate_s),
                        self.disp_bytes_sec(bytes_s),
                        self.disp_resp_avg(resp_a),
                        self.disp_failure_count(failures),
                        self.elapsed_time(),
                    )
                )

                try:
                    if final:
                        resp_cv = statistics.covariance
                        resp_a = statistics.mean(self.operation_hist[op]["resp"])
                        resp_sd = statistics.stdev(self.operation_hist[op]["resp"])
                        respdev_col = self.dev_color(resp_a, resp_sd)
                        

                        sys.stdout.write("\n")
                        sys.stdout.write("\u2502" + " "*45)
                        sys.stdout.write("{0}stability (CV): {1}{2:>8}{3}".format(
                            bcolors.ITALIC+bcolors.GRAY, respdev_col, "{:.2f}%".format((resp_sd/resp_a)*100), bcolors.ENDC)
                        )
                        sys.stdout.write("\n\u2502")
                except KeyError:
                    # If a stage stops immediately, then there won't be an
                    # operations history. Give up.
                    pass
                

        if final:
            sys.stdout.write("\n")
        sys.stdout.flush()

        del stat_sample

        # print(self.stat_rotation)
        self.last_show = now


    def readmap_progress(self, x, outof, final=False):

        if (x % 1000) == 0 or final:
            nownow = time.time()
        else:
            return

        perc = 1 if final else (x / outof)
        try:
            rate = (x - self.last_rm_count) / (nownow - self.last_rm_time)
        except ZeroDivisionError:
            rate = 0

        sys.stdout.write("\r\u2502 readmap: ")
        self.progress(perc, final=final)
        sys.stdout.write(
            " {}".format("[{:>7} op/s]".format(human_readable(rate, print_units="ops")))
        )

        if final:
            sys.stdout.write("\n\u2502\n")

        sys.stdout.flush()
        self.last_rm_time = nownow
        self.last_rm_count = x


    def dunno(self, width=10, final=False, color=""):
        blocks = (
            "\u258F", "\u258E",
            "\u258D", "\u258C",
            "\u258B", "\u258A",
            "\u2589", "\u2588",
            "\u2589", "\u258A",
            "\u258B", "\u258C",
            "\u258D", "\u258E",
            "\u258F")
        
        if final:
            sys.stdout.write(
                "{}{} 100%{}".format(bcolors.GRAY, "\u2588" * width, bcolors.ENDC)
            )
        else:
            for b in range(0, width):
                sys.stdout.write(blocks[self.progress_indx % len(blocks)])
                self.progress_indx += 1
            sys.stdout.write(" ???%")

    def progress(self, perc, width=10, final=False, color=""):
        if final or perc > 1:
            perc = 1
        blocks = (
            "\u258F",
            "\u258E",
            "\u258D",
            "\u258C",
            "\u258B",
            "\u258A",
            "\u2589",
            "\u2588",
        )  # eighth blocks
        fillchar = "\u2588"
        char_w = perc * width
        leading_char = blocks[math.floor((char_w * 8) % 8)]
        
        if final:
            color = bcolors.GRAY
            leading_char = " "
        if self.reread > 0:
            color = bcolors.WARNING

        sys.stdout.write(
            "{}{}{}{}{:>3}%{}".format(
                color,
                fillchar * (math.floor(char_w)),
                leading_char,
                " " * (width - math.floor(char_w)),
                math.ceil(perc * 100),
                bcolors.ENDC,
            )
        )

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
        return "[{:>8}]".format(
            "{:02}:{:02}:{:02}".format(int(hours), int(minutes), int(seconds))
        )


    def disp_bytes_sec(self, bytes_sec):
        return "[{:>10}/s]".format(human_readable(bytes_sec))


    def disp_ops_sec(self, ops_sec, color=""):
        h_rate = human_readable(ops_sec, print_units="ops")
        return "[{}{:>7} op/s{}]".format(
            color, h_rate, bcolors.ENDC if color != "" else ""
        )


    def disp_resp_avg(self, resp_avg):
        return "[{:>9.2f} ms]".format(resp_avg * 1000)


    def disp_failure_count(self, count=0):
        return "[{:>8} ttl]".format(human_readable(count, print_units="ops"))
