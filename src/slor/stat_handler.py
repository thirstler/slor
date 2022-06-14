from operator import ne
from slor.shared import *
from slor.sample import perfSample
from slor.output import *
import time
import math
import statistics

class statHandler:

    config = None
    standing_sample = {}
    operations = None
    last_show = 0
    stage = None
    stage_class = None
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

    def __del__(self):
        self.standing_sample.clear()

    def __init__(self, config, stage, duration):
        self.config = config
        self.operations = ()
        self.stage = stage
        self.stage_class = opclass_from_label(stage)
        self.stage_start_time = time.time()
        self.progress_start_time = self.stage_start_time
        self.duration = duration
        self.reread = 0
        self.operation_hist = {}

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


    def show(self, final=False):

        # signal if we can start using figures for calculating a final average
        # for dispaly.
        calc_avg = False 

        # Determine if it's time to show the next sample or not
        now = time.time()
        if (self.last_show + SHOW_STATS_RATE) >= now and final != True:
            return

        if self.stage == "init":
            sys.stdout.write("\r\u2502 init:")
            if final:
                sys.stdout.write("    done\n")
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

        # Multiple operations in the sample (mixed)
        if len(stat_sample.operations) > 1:

            if self.headers:
                row_data = [("\u2502", 26, "", "<")]
                for op in stat_sample.operations:
                    row_data.append((op, 14, "", ">"))
                row_data.append(("total", 14, "", ">"))
                row_data.append(("elapsed", 10, "", ">"))
                sys.stdout.write(format_row(row_data, replace=False, newline=True, padding=1))
                self.headers = False

            progress_chars = ""

            # Workloads with time limit (any benchmark workload)
            if self.stage_class in PROGRESS_BY_TIME:
                perc = 0
                # Nothing reported in yet set values
                if stat_sample.sample_seq == 0:
                    self.progress_start_time = time.time()
                else:
                    perc = (time.time() - self.progress_start_time) / self.duration

                if perc > 1:
                    perc = 1  # Just in case

                progress_chars = self.progress(perc, final=final, printme=False)

            # Work-to-finish workloads (won't happen w/mixed workload)
            elif self.stage_class in PROGRESS_BY_COUNT:
                progress_chars = self.progress(stat_sample.percent_complete(), final=final, printme=False)

            # Start global I/O history
            if not "global_rate" in self.operation_hist:
                self.operation_hist["global_rate"] = []
            self.operation_hist["global_rate"].append(stat_sample.get_workload_io_rate())
            
            # Record op history and show each operation response time
            row_data = [
                ("\u2502", 1, "", "<"),
                (self.stage_class+":", 8, "", ">"),
                (progress_chars, 18, "", "<")
            ]
            for op in self.operations:
                
                if calc_avg and not final:
                    if op not in self.operation_hist:
                        self.operation_hist[op] = {
                            "ios": [],
                            "bytes": [],
                            "resp": [],
                            "failures": []
                        }
                    self.operation_hist[op]["ios"].append(stat_sample.get_rate("ios", op))
                    self.operation_hist[op]["bytes"].append(stat_sample.get_rate("bytes", op))
                    self.operation_hist[op]["resp"].append(stat_sample.get_resp_avg(op))
                    self.operation_hist[op]["failures"].append(stat_sample.get_metric("failures", op))
                
                if final:
                    row_data.append(
                        (self.disp_resp_avg(statistics.mean(self.operation_hist[op]["resp"])), 14, "", "<")
                    )
                else:
                    row_data.append(
                        (self.disp_resp_avg(stat_sample.get_resp_avg(op)), 14, "", "<")
                    )

            # show total ops/s
            if final:
                row_data.append(
                    (self.disp_ops_sec(statistics.mean(self.operation_hist["global_rate"])), 14, "", "<")
                )
            else:
                row_data.append(
                    (self.disp_ops_sec(stat_sample.get_workload_io_rate()), 14, "", "<")
                )
            
            row_data.append(
                (self.elapsed_time(), 10, "", ">")
            )

            sys.stdout.write(format_row(row_data, replace=False, padding=1))
            
            if final:
                sys.stdout.write("\n")
                sys.stdout.write("\u2502\n")
                sys.stdout.write(format_row(
                    [
                        ("\u2502", 1, "", "<"),
                        ("results:", 24, bcolors.BOLD, ">", ),
                        ("throughput", 14, "", ">"),
                        ("bandwidth", 14, "", ">"),
                        ("resp ms", 14, "", ">"),
                        ("failures", 14, "", ">"),
                        ("CV", 8, "", ">")
                    ],
                    replace=False,
                    padding=1,
                    newline=True
                ))
                for op in self.operation_hist:
                    if op == "global_rate": continue
                    rate_s = statistics.mean(self.operation_hist[op]["ios"])
                    bytes_s = statistics.mean(self.operation_hist[op]["bytes"])
                    resp_a = statistics.mean(self.operation_hist[op]["resp"])
                    resp_sd = statistics.stdev(self.operation_hist[op]["resp"])
                    failures = stat_sample.get_metric("failures", op)
                    respdev_col = self.dev_color(resp_a, resp_sd)
                    sys.stdout.write(format_row(
                        [
                            ("\u2502", 1, "", "<"),
                            (op+":", 24, "", ">", ),
                            (self.disp_ops_sec(rate_s), 14, "", ">"),
                            (self.disp_bytes_sec(bytes_s), 14, "", ">"),
                            (self.disp_resp_avg(resp_a), 14, "", ">"),
                            (self.disp_failure_count(failures), 14, "", ">"),
                            ("{}{:>7.2f}%{}".format(respdev_col, (resp_sd/resp_a)*100, bcolors.ENDC), 8, "", "<")
                        ],
                        replace=False,
                        padding=1,
                        newline=True
                    ))

                box_line("\n")

        # Discrete operation
        else:

            if self.headers:
                sys.stdout.write(format_row(
                    [
                        ("\u2502", 26, "", "<"),
                        ("throughput", 14, "", ">"),
                        ("bandwidth", 14, "", ">"),
                        ("resp ms", 14, "", ">"),
                        ("failures", 14, "", ">"),
                        ("elapsed", 10, "", ">")
                    ],
                    replace=False,
                    padding=1,
                    newline=True
                ))
                self.headers = False

            progress_chars = ""
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
                progress_chars = self.progress(perc, final=final, printme=False)

            # Work-to-finish workloads (prepare, blowout)
            elif self.stage_class in PROGRESS_BY_COUNT:
                progress_chars = self.progress(stat_sample.percent_complete(), final=final, printme=False)

            # Unknown terminus (cleanup)
            elif self.stage_class in UNKNOWN_PROGRESS:
                progress_chars = self.dunno(final=final, printme=False)

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

                sys.stdout.write(format_row(
                    [
                        ("\u2502", 1, "", "<"),
                        (self.stage_class+":", 8, "", ">", ),
                        (progress_chars, 18, "", ">"),
                        (self.disp_ops_sec(rate_s), 14, "", ">"),
                        (self.disp_bytes_sec(bytes_s), 14, "", ">"),
                        (self.disp_resp_avg(resp_a), 14, "", ">"),
                        (self.disp_failure_count(failures), 14, "", ">"),
                        (self.elapsed_time(), 10, "", ">")
                    ],
                    replace=False,
                    padding=1
                ))

                try:
                    if final:

                        # Keep from crashing if the stage does nothing
                        try:
                            resp_a = statistics.mean(self.operation_hist[op]["resp"])
                            resp_sd = statistics.stdev(self.operation_hist[op]["resp"])
                            resp_cv = (resp_sd/resp_a) * 100
                            respdev_col = self.dev_color(resp_a, resp_sd)
                        except:
                            resp_a = 0
                            resp_sd = 0
                            resp_cv = 0
                            respdev_col = bcolors.GRAY

                        sys.stdout.write("\n")
                        if all(self.stage_class != x for x in ("prepare", "cleanup")):
                            #box_line("         {0}instability (CV) ".format(bcolors.ITALIC+bcolors.GRAY))
                            #sys.stdout.write(" "*30 + "{}{}{}".format(respdev_col, "{:>12.2f}%".format(resp_cv), bcolors.ENDC))
                            #sys.stdout.write("\n")
                            sys.stdout.write(format_row(
                                [
                                    ("\u2502", 1, "", "<"),
                                    ("instability (CV):", 54, bcolors.ITALIC+bcolors.GRAY, ">"),
                                    ("{:>12.2f}%".format(resp_cv), 14, bcolors.ITALIC+bcolors.GRAY, ">")

                                ],
                                newline=True,
                                padding=1
                            ))
                        box_line("\n")
                        
                except KeyError:
                    # If a stage stops immediately, then there won't be an
                    # operations history. Give up.
                    pass
                

        #if final:
        #    sys.stdout.write("\n")
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

    def progress(self, perc, width=10, final=False, color="", printme=True):
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

        characters = "{}{}{}{}{:>3}%{}".format(
                color,
                fillchar * (math.floor(char_w)),
                leading_char,
                " " * (width - math.floor(char_w)),
                math.ceil(perc * 100),
                bcolors.ENDC,
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
        return "[{:>8}]".format(
            "{:02}:{:02}:{:02}".format(int(hours), int(minutes), int(seconds))
        )


    def disp_bytes_sec(self, bytes_sec, width=14):
        width=width-4
        return "[{0:>{1}}/s]".format(human_readable(bytes_sec), str(width))


    def disp_ops_sec(self, ops_sec, color="", width=14):
        width=width-7
        h_rate = human_readable(ops_sec, print_units="ops")
        return "[{0}{1:>{3}} op/s{2}]".format(
            color, h_rate, bcolors.ENDC, str(width)
        )


    def disp_resp_avg(self, resp_avg, width=14):
        width=width-5
        return "[{0:>{1}.2f} ms]".format(resp_avg * 1000, str(width))


    def disp_failure_count(self, count=0, width=14):
        width=width-6
        return "[{0:>{1}} ttl]".format(human_readable(count, print_units="ops"), str(width))
