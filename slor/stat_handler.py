from pyclbr import Function
import time, os
from shared import *
import math

class statHandler:
    
    config = None
    standing_sample = {}
    global_sample = {}
    operations = None
    last_show = 0
    stage = None
    count_target = 0
    global_counter = 0
    progress_start_time = 0
    last_rm_count = 0
    last_rm_time = 0
    stat_rotation = float(0)
    stat_types = ("throughput", "bandwidth", "response")
    last_stage = None

    def __init__(self, config, stage):
        self.config = config
        self.operations = ()
        self.stage = stage
        self.progress_start_time = time.time()

        if self.stage == "mixed":
            for s in config["mixed_profile"]:
                self.operations += (s,)
        else:
            if any(stage == x for x in ("prepare", "blowout")):
                self.operations = ("write",)
            else:
                self.operations = (self.stage,)

        for w in self.config["driver_list"]:
            w = w["host"]
            self.standing_sample[w] = {}
            for p in range(0, self.config["driver_proc"]):
                self.standing_sample[w][p] = sample_structure(self.operations)
                self.standing_sample[w][p]["walltime"] = 0
                self.standing_sample[w][p]["iotime"] = 0
                self.standing_sample[w][p]["wrkld_eff"] = 0

    def set_count_target(self,count):
        self.count_target = count

    def update_standing_sample(self, data):
        self.standing_sample[data["w_id"]][data["t_id"]] = data["value"]

        # Add a wall-time figure
        self.standing_sample[data["w_id"]][data["t_id"]]["walltime"] = \
            self.standing_sample[data["w_id"]][data["t_id"]]["end"] - \
                self.standing_sample[data["w_id"]][data["t_id"]]["start"]

        # Add IO time for all operations and a global count figure
        iotime = 0
        for o in self.standing_sample[data["w_id"]][data["t_id"]]["st"]:
            iotime += self.standing_sample[data["w_id"]][data["t_id"]]["st"][o]["iotime"]
            self.global_counter += self.standing_sample[data["w_id"]][data["t_id"]]["st"][o]["ios"]
        self.standing_sample[data["w_id"]][data["t_id"]]["iotime"] = iotime

        try:
            # Add a benchmark process efficiency figure
            self.standing_sample[data["w_id"]][data["t_id"]]["wrkld_eff"] = \
                self.standing_sample[data["w_id"]][data["t_id"]]["iotime"] / \
                self.standing_sample[data["w_id"]][data["t_id"]]["walltime"]
        except:
            self.standing_sample[data["w_id"]][data["t_id"]]["wrkld_eff"] = 0

    def mk_global_sample(self):
        self.global_sample.clear()
        self.global_sample = sample_structure(self.operations)
        self.global_sample["walltime"] = 0
        self.global_sample["iotime"] = 0
        self.global_sample["wrkld_eff"] = 0
        self.global_sample["perc"] = 0

        # Process count for the cleanup stageonly uses 1 worker per bucket
        if self.stage == "cleanup":
            count = self.config["bucket_count"]
        else:
            count = len(self.config["driver_list"]) * self.config["driver_proc"]

        for w in self.standing_sample:
            for t in self.standing_sample[w]:
                self.global_sample["start"] += self.standing_sample[w][t]["start"]
                self.global_sample["end"] += self.standing_sample[w][t]["end"]
                self.global_sample["walltime"] += self.standing_sample[w][t]["walltime"]
                self.global_sample["iotime"] += self.standing_sample[w][t]["iotime"]
                self.global_sample["wrkld_eff"] += self.standing_sample[w][t]["wrkld_eff"]
                self.global_sample["perc"] += self.standing_sample[w][t]["perc"]
                self.global_sample["ios"] += self.standing_sample[w][t]["ios"]
                try:
                    for op in self.standing_sample[w][t]["st"]:
                        self.global_sample["st"][op]["resp"] += self.standing_sample[w][t]["st"][op]["resp"]
                        self.global_sample["st"][op]["bytes"] += self.standing_sample[w][t]["st"][op]["bytes"]
                        self.global_sample["st"][op]["ios"] += self.standing_sample[w][t]["st"][op]["ios"]
                        self.global_sample["st"][op]["failures"] += self.standing_sample[w][t]["st"][op]["failures"]
                        self.global_sample["st"][op]["iotime"] += self.standing_sample[w][t]["st"][op]["iotime"]
                except: pass

        # fix global figures
        self.global_sample["start"] /= count
        self.global_sample["end"] /= count
        self.global_sample["walltime"] /= count
        self.global_sample["iotime"] /= count
        self.global_sample["wrkld_eff"] /= count
        self.global_sample["perc"] /= count

    def rotate_mixed_stat_func(self, final):
        if final: self.stat_rotation = 0
        if self.stat_types[int(self.stat_rotation)] == "throughput":
            disp_func=self.ops_sec
        elif self.stat_types[int(self.stat_rotation)] == "bandwidth":
            disp_func=self.bytes_sec
        elif self.stat_types[int(self.stat_rotation)] == "response":
            disp_func=self.resp_avg
        self.stat_rotation += 0.20
        if self.stat_rotation >= 3:
            self.stat_rotation = 0
        return disp_func


    def show(self, final=False):
        now = time.time()

        if (self.last_show + SHOW_STATS_RATE) >= now and final != True:
            return

        if self.stage == "init" and final:
            sys.stdout.write("\r\u2502 init:    done")
            if final: sys.stdout.write("\n")
            sys.stdout.flush()
            return
        elif self.stage == "init":
            sys.stdout.write("\r\u2502 init:")
            if final: sys.stdout.write("\n")
            sys.stdout.flush()
            return
            
        self.mk_global_sample()
        sys.stdout.write("\r")

        sys.stdout.write("\u2502 {:<9}".format(self.stage + ":"))
        if len(self.operations) > 1:
            if self.stage in PROGRESS_BY_TIME:
                perc = (time.time()-self.progress_start_time)/self.config["run_time"]
                if perc > 1: perc = 1
                self.progress(perc, final=final)
            elif self.stage in PROGRESS_BY_COUNT:
                self.progress(self.global_sample["perc"], final=final)
            
            stat_func = self.rotate_mixed_stat_func(final)
            for o in self.operations:
                sys.stdout.write(" {}".format(stat_func(operation=o)))
            
            sys.stdout.write(" {}".format(self.elapsed_time()))

        else:

            # Single operation

            if self.stage in PROGRESS_BY_TIME:
                perc = (time.time()-self.progress_start_time)/self.config["run_time"]
                if perc > 1: perc = 1
                self.progress(perc, final=final)
            elif self.stage in PROGRESS_BY_COUNT:
                self.progress(self.global_sample["perc"], final=final)
            elif self.stage in UNKNOWN_PROGRESS:
                self.dunno(final=final)

            sys.stdout.write(" {} {} {} {} {}".format(
                self.ops_sec(operation=self.operations[0]),
                self.bytes_sec(operation=self.operations[0]),
                self.resp_avg(operation=self.operations[0]),
                self.failure_count(operation=self.operations[0]),
                self.elapsed_time()
            ))

        if final: sys.stdout.write("\n")
        sys.stdout.flush()


        
        #print(self.stat_rotation)
        self.last_show = now

        
    def readmap_progress(self, x, outof, final=False):

        if (x % 100) == 0 or final == True:
            nownow = time.time()
        else: return

        perc = 1 if final else (x/outof)
        try:
            rate = (x - self.last_rm_count)/(nownow-self.last_rm_time)
        except ZeroDivisionError:
            rate = 0

        sys.stdout.write("\r\u2502 readmap: ")
        self.progress(perc, final=final)
        sys.stdout.write(" {}".format(
            "[{:>7} op/s]".format(human_readable(rate, print_units="ops"))
        ))
        
        if final: 
            sys.stdout.write("\n")

        sys.stdout.flush()
        self.last_rm_time = nownow
        self.last_rm_count = x

    def dunno(self, width=10, final=False):
        blocks = ("\u2596", "\u2597", "\u2598", "\u2599", "\u259A", "\u259B", "\u259C", "\u259D", "\u259E", "\u259F", "\u25E2", "\u25E3", "\u25E4", "\u25E5")
        if final:
            sys.stdout.write("\u2592" * width)
            sys.stdout.write(" 100%")
        else:
            for b in range(0, width):
                sys.stdout.write(blocks[random.randint(0,13)])
            sys.stdout.write("   ?%")
            

    def progress(self, perc, width=10, final=False):
        if final: perc = 1
        blocks = ("\u258F", "\u258E", "\u258D", "\u258C", "\u258B", "\u258A", "\u2589", "\u2588") # eighth blocks
        fillchar = u"\u2588"
        char_w = perc*width
        leading_char = blocks[math.floor((char_w*8) % 8)]
        if final:
            fillchar = u"\u2592"
            leading_char = " "
        sys.stdout.write(u"{}{}{}{:>3}%".format(fillchar*(math.floor(char_w)), leading_char, " "*(width-math.floor(char_w)), math.ceil(perc*100)))


    def failure_count(self, stat_sample=None, operation=None):
        if not stat_sample:
            stat_sample = self.global_sample

        if stat_sample["walltime"] == 0:
            rate = 0
        else:   
            rate = human_readable(stat_sample["st"][operation]["failures"], print_units="ops")
        return("[{:>8} ttl]".format(rate))

    def elapsed_time(self):
        elapsed = time.time() - self.progress_start_time
        hours, remainder = divmod(elapsed, 3600)
        minutes, seconds = divmod(remainder, 60)
        return("[{:>8}]".format('{:02}:{:02}:{:02}'.format(int(hours), int(minutes), int(seconds))))

    def bytes_sec(self, stat_sample=None, operation=None, width=None):
        if not stat_sample:
            stat_sample = self.global_sample

        if stat_sample["walltime"] == 0:
            rate = 0
        else:
            rate = human_readable(stat_sample["st"][operation]["bytes"]/stat_sample["walltime"])
        return("[{:>10}/s]".format(rate))


    def ops_sec(self, stat_sample=None, operation=None, width=None):
        color=""
        if not stat_sample:
            stat_sample = self.global_sample

        if stat_sample["walltime"] == 0:
            h_rate = 0
        else:
            float_rate = stat_sample["st"][operation]["ios"]/stat_sample["walltime"]
            if float_rate > self.config["iop_limit"] and any(operation == x for x in ("read", "delete")):
                color=bcolors.WARNING
                
            h_rate = human_readable(float_rate, print_units="ops")
        return("[{}{:>7} op/s{}]".format(color, h_rate,bcolors.ENDC if color != "" else ""))

    def resp_avg(self, stat_sample=None, operation=None, value=None, width=None):
        resp_t = 0
        if not stat_sample:
            stat_sample = self.global_sample

        if stat_sample["walltime"] == 0:
            resp_t = 0
        elif value:
            resp_t = value
        else:
            for r in stat_sample["st"][operation]["resp"]:
                resp_t += r
            try:
                resp_t = resp_t/len(stat_sample["st"][operation]["resp"])
            except:
                resp_t = 0

        return("[{:>9.2f} ms]".format(resp_t*1000))
