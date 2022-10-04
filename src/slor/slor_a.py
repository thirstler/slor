from slor.shared import *
from slor.sample import *
from slor.output import *
import sqlite3
import json
import datetime
import statistics
import pickle
import zlib
from  datetime import datetime
        
class SlorAnalysis:
    conn = None
    stages = None
    workers = None
    processes = {}
    stats = {}
    not_workload = (
        "prepare",
        "blowout",
        "cleanup",
    )  # Skip theses stages when doing analysis
    stage_itr = {}  # handle multiple stages with the same name
    db_file = None
    histogram_partitions = None
    histogram_percentile = None

    def __init__(self, args):

        self.db_file = args.input
        self.conn = sqlite3.connect(args.input)
        self.histogram_partitions = int(args.histogram_partitions)
        self.histogram_percentile = float(args.histogram_percentile)

    def __del__(self):
        if self.conn:
            self.conn.close()

    def export_csv(self):
        pass

    def format_key_value(
        self, key, value, l_col=26, r_col=18, valcolor="\0\0\0\0\0\0\0"
    ):
        return "{2:<{0}}{4}{3:<{1}}{5}".format(
            l_col, r_col, key, value, valcolor, bcolors.ENDC
        )

    def print_basic_stats(self):

        print(BANNER)

        stats = self.get_all_basic_stats()
        global_config = self.get_config()[0]
        obj_sz_range = sizeRange(low=global_config["sz_range"]["low"], high=global_config["sz_range"]["high"])
        key_sz_range = sizeRange(low=global_config["key_sz"]["low"], high=global_config["key_sz"]["high"])

        sys.stdout.write("GLOBAL CONFIGURATION\n\n")
        left = []
        left.append(self.format_key_value("Workload name:", global_config["name"]))
        left.append(
            self.format_key_value("Stage runtime:", global_config["run_time"])
        )
        left.append(
            self.format_key_value("Bucket prefix:", global_config["bucket_prefix"])
        )
        left.append(
            self.format_key_value(
                "Bucket count:", global_config["bucket_count"]
            )
        )
        left.append(
            self.format_key_value(
                "Object size config:",
                (
                    "{} - {}  avg: {}".format(
                        human_readable(obj_sz_range.low),
                        human_readable(obj_sz_range.high),
                        human_readable(obj_sz_range.avg),
                    )
                    if obj_sz_range.low != obj_sz_range.high
                    else human_readable(obj_sz_range.low)
                ),
            )
        )
        left.append(
            self.format_key_value(
                "Key length config:",
                "{} - {}  avg: {}".format(
                    key_sz_range.high,
                    key_sz_range.low,
                    key_sz_range.avg,
                )
                if key_sz_range.low != key_sz_range.high
                else key_sz_range.low,
            )
        )
        if global_config["get_range"]:
            left.append(self.format_key_value(
                "Get-range raw config", str(global_config["get_range"])
            ))

        right = []
        right.append("Drivers:")
        for b in global_config["driver_list"]:
            right.append(
                "  {}{}:{}{}".format(bcolors.BOLD, b["host"], b["port"], bcolors.ENDC)
            )
        right.append(
            self.format_key_value(
                "Processes per driver:",
                "{} ({} total)".format(
                    global_config["driver_proc"],
                    global_config["driver_proc"] * len(global_config["driver_list"]),
                ),
            )
        )
        right.append(
            self.format_key_value(
                "Cache overrun target:", human_readable(global_config["ttl_sz_cache"])
            )
        )
        right.append(
            self.format_key_value(
                "Prepared data size:", human_readable(global_config["ttl_prepare_sz"])
            )
        )
        right.append("IOs/sec target:")
        right.append(
            "  {:20}".format(
                "{}{}{} ops ({} ops x {} sec x {} = {})".format(
                    bcolors.BOLD,
                    human_readable(global_config["iop_limit"], print_units="ops"),
                    bcolors.ENDC,
                    human_readable(global_config["iop_limit"], print_units="ops"),
                    global_config["run_time"],
                    human_readable(global_config["sz_range"]["avg"]),
                    human_readable(
                        global_config["run_time"]
                        * global_config["iop_limit"]
                        * global_config["sz_range"]["avg"]
                    ),
                )
            )
        )

        text = ""
        for z in range(0, max(len(left), len(right))):
            if z not in left:
                left.append(self.format_key_value("", ""))
            if z not in right:
                right.append(self.format_key_value("", ""))
            text += "{:<50} {:<30}\n".format(left[z], right[z])

        indent(text, indent=2)

        sys.stdout.write("WORKLOAD STATISTICS\n\n")

        for stage in stats:

            s_config = self.get_config(stage=stage)
            rmkeys = 0
            for driver in s_config:
                if "readmap" in driver:
                    rmkeys += len(driver["readmap"])
                    driver["readmap"].clear()
            
            # Start new "test" string #
            sys.stdout.write("─"*32 + "┤ " + "STAGE: {:>11}".format(stage) + " ├" + "─"*32 + "\n\n")
            text = ""
            left = []
            try:
                s_config = s_config[0]
            except IndexError:
                print("bad stage key: " + stage)
                print(json.dumps(s_config, indent=2))
                exit()

            left.append(self.format_key_value("Stage configuration:", ""))
            left.append(self.format_key_value("Run time:", s_config[0]["run_time"]))
            left.append(self.format_key_value("Prepared objects:", "~"+ str(global_config["prepare_objects"])))
            right = []
            left.append(
                self.format_key_value(
                    "Object size config:",
                    (
                        "{} - {}  (avg: {})".format(
                            human_readable(s_config[0]["sz_range"]["low"]),
                            human_readable(s_config[0]["sz_range"]["high"]),
                            human_readable(s_config[0]["sz_range"]['avg']),
                        )
                        if s_config[0]["sz_range"]["low"] != s_config[0]["sz_range"]["high"]
                        else (human_readable(s_config[0]["sz_range"]["low"]))
                    ),
                )
            )
            if global_config["get_range"]:
                '''
                cfg = ""
                avg = 0
                for x in global_config["get_range"]:
                    avg += x
                    cfg += human_readable(x) + " - "
                if int(avg/len(global_config["get_range"])) != global_config["get_range"][0]:
                    cfg += human_readable(avg/len(global_config["get_range"])) + " (avg),  "

                left.append(self.format_key_value(
                    "Get-range config", cfg[:-3]
                ))
                '''

                left.append(
                    self.format_key_value(
                        "Get range config:",
                        (
                            "{} - {}  (avg: {})".format(
                                human_readable(s_config[0]["get_range"]["low"]),
                                human_readable(s_config[0]["get_range"]["high"]),
                                human_readable(s_config[0]["get_range"]["avg"]),
                            )
                            if s_config[0]["get_range"]["low"] != s_config[0]["get_range"]["high"]
                            else (human_readable(s_config[0]["get_range"]["low"]))
                        ),
                    )
                )

            left.append(
                self.format_key_value(
                    "Key length config:",
                    "{} - {}  avg: {}".format(
                        s_config[0]["key_sz"]["low"],
                        s_config[0]["key_sz"]["high"],
                        s_config[0]["key_sz"]["avg"],
                    )
                    if s_config[0]["key_sz"]["low"] != s_config[0]["key_sz"]["high"]
                    else s_config[0]["key_sz"]["low"],
                )
            )
            left.append(
                self.format_key_value("Bucket count:", s_config[0]["bucket_count"])
            )
            if stage[:5] == "mixed":
                left.append("Mixed profile config:")
                left.append("  " + json.dumps(s_config[0]["mixed_profile"]))

            for z in range(0, max(len(left), len(right))):
                if z not in left:
                    left.append("")
                if z not in right:
                    right.append("")
                text += "{:<50} {:<30}\n".format(left[z], right[z])

            text += "\nAggregate Stage Stats (all operations)\n"
            text += "     Window: {0}{1:<12.2f}{2} {3}- analysed sample window in seconds{2}\n".format(
                bcolors.BOLD,
                stats[stage]["global"]["window"],
                bcolors.ENDC,
                bcolors.GRAY,
            )
            text += "   I/O time: {0}{1:<12.2f}{2} {3}- cumulative time spent during I/O (vs other processesing){2}\n".format(
                bcolors.BOLD,
                stats[stage]["global"]["iotime"],
                bcolors.ENDC,
                bcolors.GRAY,
            )
            text += "  Wall time: {0}{1:<12.2f}{2} {3}- cumulative wall time of workers{2}\n".format(
                bcolors.BOLD,
                stats[stage]["global"]["walltime"],
                bcolors.ENDC,
                bcolors.GRAY,
            )
            text += "  Efficency: {0}{1:<12.2f}{2} {3}- workload efficency (io-time/wall-time){2}\n".format(
                bcolors.BOLD,
                stats[stage]["global"]["wrkld_eff"],
                bcolors.ENDC,
                bcolors.GRAY,
            )
            text += "   IO count: {0}{1:<12}{2} {3}- sum of all IO operations in this stage{2}\n".format(
                bcolors.BOLD,
                human_readable(stats[stage]["global"]["ios"], print_units="ops"),
                bcolors.ENDC,
                bcolors.GRAY,
            )
            text += "      Ops/s: {0}{1:<12}{2} {3}- global operations per second{2}\n\n".format(
                bcolors.BOLD,
                human_readable(
                    stats[stage]["global"]["ios"] / stats[stage]["global"]["window"],
                    print_units="ops",
                ),
                bcolors.ENDC,
                bcolors.GRAY,
            )
            
            stage_range = self.get_stage_range(stage)
            for i, operation in enumerate(stats[stage]["operations"]):
                
                alias = stats[stage]["operations"][operation]

                text += "Operation class: {}{}{}\n".format(
                    bcolors.BOLD, operation, bcolors.ENDC
                )

                text += "\nResponse time distribution, ({:.2f}% percentile):\n\n".format(self.histogram_percentile*100)
                text += histogram_graph(alias["histogram"], height=10, units="ms", h_tickers=6)
                text += "\n"

                left = []
                left.append(
                    self.format_key_value(
                        "  Workload sample len.:",
                        "{} sec".format((stage_range[1] - stage_range[0]) / 1000),
                    )
                )
                left.append(
                    self.format_key_value(
                        "  Average operation size:",
                        human_readable(alias["ttl_bytes"] / alias["ttl_operations"]),
                    )
                )
                left.append(
                    self.format_key_value(
                        "  Bandwidth:",
                        "{}/s".format(human_readable(alias["bytes/s"])),
                        valcolor=bcolors.BOLD,
                    )
                )
                left.append(
                    self.format_key_value(
                        "  Average ops/s:",
                        human_readable(alias["ios/s"], print_units="ops", precision=4),
                        valcolor=bcolors.BOLD,
                    )
                )
                left.append(
                    self.format_key_value(
                        "  Total bytes:", human_readable(alias["ttl_bytes"], precision=4)
                    )
                )
                left.append(
                    self.format_key_value(
                        "  Total operations:",
                        human_readable(alias["ttl_operations"], print_units="ops", precision=4),
                    )
                )
                left.append(
                    self.format_key_value(
                        "  Share of stage ops:",
                        "{:.2f}%".format(
                            (alias["ttl_operations"] / stats[stage]["global"]["ios"])
                            * 100
                        ),
                    )
                )
                left.append(
                    self.format_key_value(
                        "  Failed operations:",
                        "{} ({:.2f}%)".format(
                            human_readable(alias["failures"], print_units="ops"),
                            alias["failures"] / alias["ttl_operations"],
                        ),
                    )
                )

                right = []
                right.append(
                    self.format_key_value(
                        "Response time average:",
                        "{:.2f} ms".format(alias["resp_avg"] * 1000),
                        valcolor=bcolors.BOLD,
                    )
                )
                right.append(
                    self.format_key_value("Response time percentiles (% below):", "")
                )
                right.append(
                    self.format_key_value(
                        "  0.99999:",
                        "{:.4f} ms".format(alias["resp_perc"]["0.99999"] * 1000),
                    )
                )
                right.append(
                    self.format_key_value(
                        "  0.9999:",
                        "{:.4f} ms".format(alias["resp_perc"]["0.9999"] * 1000),
                    )
                )
                right.append(
                    self.format_key_value(
                        "  0.999:",
                        "{:.4f} ms".format(alias["resp_perc"]["0.999"] * 1000),
                    )
                )
                right.append(
                    self.format_key_value(
                        "  0.99:", "{:.4f} ms".format(alias["resp_perc"]["0.99"] * 1000)
                    )
                )
                right.append(
                    self.format_key_value(
                        "  0.9:", "{:.4f} ms".format(alias["resp_perc"]["0.9"] * 1000)
                    )
                )
                right.append(
                    self.format_key_value(
                        "  0.5 (median):",
                        "{:.4f} ms".format(alias["resp_perc"]["0.5"] * 1000),
                    )
                )
                right.append(
                    self.format_key_value(
                        "Standard deviation:",
                        "{:.4f} ms".format(alias["resp_stddiv"] * 1000),
                    )
                )
                right.append(
                    self.format_key_value(
                        "Coefficient of variation:",
                        "{:.2f}".format(alias["resp_stddiv"]/alias["resp_avg"]),
                    )
                )
                for z in range(0, max(len(left), len(right))):
                    if z not in left:
                        left.append(self.format_key_value("", ""))
                    if z not in right:
                        right.append(self.format_key_value("", ""))
                    text += "{:<50} {:<30}\n".format(left[z], right[z])

                if (i + 1) < len(stats[stage]["operations"]):
                    text += "\n"
                
            indent(text, indent=2)

    def get_all_basic_stats(self):
        stats = {}
        self.stages = self.get_stages()

        for stage in self.stages:
            if opclass_from_label(stage) in self.not_workload:
                continue
            stage_range = self.get_stage_range(stage)
            stats[stage] = self.get_stats(stage, stage_range[0], stage_range[1])
        return stats

    def dump_csv(self, raw=False):
        config = self.get_config(stage="global")[0]
        sys.stdout.write("CONFIGURATION:\n")
        for key in config:
            sys.stdout.write("{},\"{}\"\n".format(key, str(config[key])))
        for stage in self.get_stages():

            sys.stdout.write("\n")

            if opclass_from_label(stage) in self.not_workload:
                continue

            sys.stdout.write("STAGE,{}\n".format(stage))
            stage_range = self.get_stage_range(stage)
            series = self.get_series(stage, stage_range[0], stage_range[1])
            stats = self.get_stats(stage, stage_range[0], stage_range[1])
            
            row = "ts,"
            for col in list(series.values())[0]:
                row += col+":ios/s,"+col+":bytes/s,"+col+":resp_ms,"
            sys.stdout.write(row[:-1]+"\n")

            for ts in series:
                row = datetime.fromtimestamp(ts).isoformat() + ","
                for op in series[ts]:
                    row += str(series[ts][op]["ios/s"]) + "," + str(series[ts][op]["bytes/s"]) + "," + str(series[ts][op]["res_t"]) + ","
                
                sys.stdout.write(row[:-1]+"\n")

            sys.stdout.write("Response time histogram (ms):\n")
            for op in stats["operations"]:
                sys.stdout.write(op+":bucket,count\n")
                for part in stats["operations"][op]["histogram"]:
                    sys.stdout.write(str(part["val"]) + "," + str(part["count"]) + "\n")

            if raw:
                sys.stdout.write("Raw response times:\n")
                for op in stats["operations"]:
                    for t in stats["operations"][op]["iotimes"]:
                        sys.stdout.write("{:.2f},".format(t*1000))
                    sys.stdout.write("\n")

    def get_tick(self, timestamp:int, quanta=STATS_QUANTA):
        return timestamp - (timestamp % quanta)


    def get_series(self, stage, start, stop):
        """
        Get sample data in timestamp order for CSV output
        """
        workers = self.get_workers()
        cur = self.conn.cursor()
        series_master = {}
        start_s = start / 1000
        stop_s = stop / 1000
        for i in range(
            self.get_tick(int(start_s)), self.get_tick(int(stop_s)) + STATS_QUANTA, STATS_QUANTA
        ):
            series_master[i] = {}

        for i, worker in enumerate(workers):
            query = 'SELECT data,ts FROM {} WHERE stage="{}" AND ts >= {} AND ts <= {} ORDER BY ts'.format(
                worker, stage, start, stop
            )
            data = cur.execute(query)
            for row in data:
                stat = pickle.loads(zlib.decompress(row[0]))
                if "final" in stat["value"]:
                    continue
                sec = int(stat["value"]["window_start"])
                dateslot = self.get_tick(sec, STATS_QUANTA)

                for op in stat["value"]["operations"]:
                    if dateslot not in series_master:
                        continue
                    if op not in series_master[dateslot]:
                        series_master[dateslot][op] = {
                            "bytes/s": 0,
                            "ios/s": 0,
                            "res_t": 0
                        }
                    series_master[dateslot][op]["bytes/s"] += \
                            stat["value"]["operations"][op]["bytes"]/(stat["value"]["window_end"]-stat["value"]["window_start"])
                    series_master[dateslot][op]["ios/s"] += \
                            stat["value"]["operations"][op]["ios"]/(stat["value"]["window_end"]-stat["value"]["window_start"])
                    if len(stat["value"]["operations"][op]["iotime"]) > 0:
                        series_master[dateslot][op]["res_t"] += \
                            statistics.mean(stat["value"]["operations"][op]["iotime"])
                    else:
                        series_master[dateslot][op]["res_t"] = None
                        
        return series_master

    def get_config(self, stage="global"):
        """
        Get benchmark run configuration
        """
        query = "SELECT * FROM config WHERE stage='{}'".format(stage)
        cur = self.conn.cursor()
        data = cur.execute(query)

        retval = []
        for row in data:
            retval.append(json.loads(row[2]))
        cur.close()
        return retval



    def get_stats(self, stage, start, stop):
        """
        One giant pass to avoid running the same queries over and over again.
        """
        workers = self.get_workers()
        cur = self.conn.cursor()
        returnval = {}
        master = perfSample()
        master.start(start_time=start / 1000)
        master.stop(stop_time=stop / 1000)
        sample_count = 0
        cumulative_wall_time = 0

        ##
        # Loop through all of the drivers
        for i, worker in enumerate(workers):
            query = 'SELECT data FROM {} WHERE stage="{}" AND ts >= {} AND ts <= {} ORDER BY ts'.format(
                worker, stage, start, stop
            )
            data = cur.execute(query)

            ##
            # Each row is a sample from a single driver
            for row in data:
                stat = pickle.loads(zlib.decompress(row[0]))
                sample = perfSample(from_json=stat["value"])
                master.merge(sample)
                sample_count += 1
                cumulative_wall_time += sample.walltime()
                del sample

        globals = {
            "window": master.walltime(),
            "ios": master.get_metric("ios"),
            "walltime": cumulative_wall_time,
            "iotime": master.iotime(),
            "sample_count": sample_count,
        }
        globals["wrkld_eff"] = globals["iotime"] / globals["walltime"]

        returnval = {"global": globals, "operations": {}}
        for op in master.get_operations():
            iotimes = master.get_metric("iotime", op)
            returnval["operations"][op] = {
                "ios/s": master.get_rate("ios", op),
                "bytes/s": master.get_rate("bytes", op),
                "failures": master.get_metric("failures", op),
                "failures/s": master.get_rate("failures", op),
                "iotime": master.iotime(op),
                "resp_avg": master.get_resp_avg(op),
                "ttl_operations": master.get_metric("ios", op),
                "ttl_bytes": master.get_metric("bytes", op),
                "resp_perc": self.get_precentiles(iotimes),
                "resp_stddiv": statistics.stdev(iotimes),
                "histogram": histogram_data(iotimes, self.histogram_partitions, min_val=0, trim=self.histogram_percentile),
                "iotimes": iotimes
            }
        cur.close()
        return returnval

    def get_precentiles(self, resp):
        resp.sort()
        list_len = len(resp)
        precentiles = (0.99999, 0.9999, 0.999, 0.99, 0.9, 0.5)
        values = {}
        for i, p in enumerate(precentiles):
            idx = int(list_len * p)
            values[str(p)] = resp[idx]
        return values

    def get_workers(self):
        if self.workers != None:
            return self.workers
        self.workers = []

        query = "SELECT name FROM sqlite_master WHERE type='table'"
        cur = self.conn.cursor()
        for x in cur.execute(query):
            if x[0] == "config":
                continue
            self.workers.append(x[0])
        cur.close()

        return self.workers

    def get_stages(self):
        if self.stages != None:
            return self.stages
        self.stages = []

        workers = self.get_workers()
        cur = self.conn.cursor()
        for worker in workers:
            for x in cur.execute(
                'SELECT DISTINCT "stage" FROM (SELECT stage, ts FROM {} ORDER BY ts)'.format(
                    worker
                )
            ):
                if x[0] not in self.stages:
                    self.stages.append(x[0])
        cur.close()

        return self.stages

    def get_processes(self, stage):
        if stage in self.processes:
            return self.processes[stage]
        self.processes[stage] = []

        workers = self.get_workers()
        for worker in workers:
            query = 'SELECT DISTINCT t_id FROM {} WHERE stage="{}"'.format(
                worker, stage
            )
            for x in self.conn.cursor().execute(query):
                self.processes[stage].append((worker, x[0]))
        return self.processes[stage]

    def get_stage_range(self, stage):
        processes = self.get_processes(stage)
        cur = self.conn.cursor()
        min_vals = []
        max_vals = []
        for process in processes:
            query = (
                'SELECT MIN(ts) AS min_ts FROM {} WHERE stage="{}" AND t_id={}'.format(
                    process[0], stage, process[1]
                )
            )
            for value in cur.execute(query):
                min_vals.append(value[0])
        min = min_vals[0]
        for v in min_vals:
            if v < min:
                min = v

        for process in processes:
            query = (
                'SELECT MAX(ts) AS max_ts FROM {} WHERE stage="{}" AND t_id={}'.format(
                    process[0], stage, process[1]
                )
            )
            for value in cur.execute(query):
                max_vals.append(value[0])
        max = max_vals[0]
        for v in min_vals:
            if v > max:
                max = v

        return (min, max)

        

