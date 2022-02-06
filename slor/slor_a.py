from re import I
import sqlite3
import json
from  shared import *

class SlorAnalysis:
    conn = None
    stages = None
    workers = None
    processes = {}
    stats = {}
    not_workload = ("prepare", "blowout", "cleanup")

    def __init__(self, args):

        self.csv_output = args.csv_out
        self.db_file = args.input
        self.conn = sqlite3.connect(args.input)
        
        
    def __del__(self):
        self.conn.close()

    def export_csv(self):
        pass

    def format_key_value(self, key, value, l_col=26, r_col=18):
        return "{2:<{0}}{4}{3:<{1}}{5}".format(l_col, r_col, key, value, bcolors.BOLD, bcolors.ENDC)

    def print_basic_stats(self):
        stats = self.get_all_basic_stats()
        global_config = self.get_config()
        print(global_config)

        print(BANNER)

        print("GLOBAL CONFIGURATION")
        left = []
        left.append(self.format_key_value("Workload name:", global_config["name"]))
        
       
        
        left.append(self.format_key_value("Default runtime:", global_config["run_time"]))
        left.append(self.format_key_value("Bucket prefix:", global_config["bucket_prefix"]))
        left.append(self.format_key_value("Default bucket count:", global_config["bucket_count"]))
        left.append(self.format_key_value("Target buckets:", ""))
        for b in range(0, global_config["bucket_count"]):
            left.append(self.format_key_value("",  "{}{}".format(global_config["bucket_prefix"], b), l_col=4,r_col=40))

        right = []
        right.append(self.format_key_value("Object size config:",
            ("{} - {}  avg: {}".format(
                human_readable(global_config["sz_range"][0]),
                human_readable(global_config["sz_range"][1]),
                human_readable(global_config["sz_range"][2]))
            if global_config["sz_range"][0] != global_config["sz_range"][1] else
                (human_readable(global_config["sz_range"][0]))
            )
        ))
        right.append(self.format_key_value("Key length config:", "{} - {}  avg: {}".format(
                global_config["key_sz"][0],
                global_config["key_sz"][1],
                global_config["key_sz"][2])
            if global_config["key_sz"][0] != global_config["key_sz"][1] else
                global_config["key_sz"][0]))
        right.append("Drivers:")
        for b in global_config["driver_list"]:
            right.append("  {}{}:{}{}".format(bcolors.BOLD,
                b["host"], b["port"], bcolors.ENDC))
        right.append(self.format_key_value("Processes per driver:", "{} ({} ttl)".format(
            global_config["driver_proc"], global_config["driver_proc"]*len(global_config["driver_list"]))))
        right.append(self.format_key_value("Cache overrun target:", global_config["ttl_sz_cache"]))
        right.append(self.format_key_value("Prepared data size:", human_readable(global_config["ttl_prepare_sz"])))
        right.append("IOs/sec target (used to calculate prepared data size):")
        right.append("  {:20}".format("{}{}{} ops ({} ops x {} sec x {} = {})".format(
                bcolors.BOLD,
                human_readable(global_config["iop_limit"], print_units="ops"),
                bcolors.ENDC,
                human_readable(global_config["iop_limit"], print_units="ops"),
                global_config["run_time"], human_readable(global_config["sz_range"][2]),
                human_readable(global_config["run_time"]*global_config["iop_limit"]*global_config["sz_range"][2]))))
        
        text = ""
        for z in range(0, max(len(left), len(right))):
            if z not in left: left.append("")
            if z not in right: right.append("")
            text += "{:<50} {:<30}\n".format(left[z], right[z])

        box_text(text)


        print("WORKLOAD STATISTICS")

        for stage in stats:

            start, stop = self.get_stage_range(stage)
            series = self.get_series(stage, start, stop)
            print(str(series))
            # Start new "test" string #
            text = "STAGE: " + stage + "\n\n"
            text += "Global Stage Stats (all operations)\n"
            text += "     Window: {0}{1:<12.2f}{2} {3}- wall time of analysed sample{2}\n".format(
                bcolors.BOLD, stats[stage]["global"]["window"], bcolors.ENDC, bcolors.GRAY)
            text += "   I/O time: {0}{1:<12.2f}{2} {3}- cumulative time spent during I/O (vs other processesing){2}\n".format(
                bcolors.BOLD, stats[stage]["global"]["iotime"], bcolors.ENDC, bcolors.GRAY)
            text += "  Wall time: {0}{1:<12.2f}{2} {3}- cumulative wall time of workers{2}\n".format(
                bcolors.BOLD, stats[stage]["global"]["walltime"], bcolors.ENDC, bcolors.GRAY)
            text += "  Efficency: {0}{1:<12.2f}{2} {3}- workload efficency (io-time/wall-time){2}\n".format(
                bcolors.BOLD, stats[stage]["global"]["wrkld_eff"], bcolors.ENDC, bcolors.GRAY)
            text += "   IO count: {0}{1:<12}{2} {3}- sum of all IO operations in this stage{2}\n".format(
                bcolors.BOLD, human_readable(stats[stage]["global"]["ios"], print_units="ops"), bcolors.ENDC, bcolors.GRAY)
            text += "      Ops/s: {0}{1:<12}{2} {3}- global operations per second{2}\n\n".format(
                bcolors.BOLD, human_readable(stats[stage]["global"]["ios"]/stats[stage]["global"]["window"],
                print_units="ops"), bcolors.ENDC, bcolors.GRAY)
            stage_range = self.get_stage_range(stage)
            for i, operation in enumerate(stats[stage]["operations"]):
                alias =  stats[stage]["operations"][operation]
                text += "Operation class: {}{}{}\n".format(bcolors.BOLD, operation, bcolors.ENDC)

                left = []
                left.append(self.format_key_value("  Workload sample len.:", (stage_range[1]-stage_range[0])/1000))
                left.append(self.format_key_value("  Average object size:", human_readable(alias["ttl_bytes"]/alias["ttl_operations"])))
                left.append(self.format_key_value("  Average bandwidth:", "{}/s".format(human_readable(alias["bytes/s"]))))
                left.append(self.format_key_value("  Total bytes:", human_readable(alias["ttl_bytes"])))
                left.append(self.format_key_value("  Average IO/s:", human_readable(alias["iops/s"], print_units="ops")))
                left.append(self.format_key_value("  Total operations:", human_readable(alias["ttl_operations"], print_units="ops")))
                left.append(self.format_key_value("  Share of stage ops:", "{:.2f}%".format((alias["ttl_operations"]/stats[stage]["global"]["ios"])*100)))
                left.append(self.format_key_value("  Failed operations:", "{} ({:.2f}%)".format(
                    human_readable(alias["failures"], print_units="ops"),
                    alias["failures"]/alias["ttl_operations"])))
  


                right = []
                right.append(self.format_key_value("Response time average:", "{:.2f} ms".format(alias["resp_avg"]*1000)))
                right.append(self.format_key_value("Response time percentiles (% below):", ""))
                right.append(self.format_key_value("  0.99999:", "{:.4f} ms".format(alias["resp_perc"]["0.99999"]*1000)))
                right.append(self.format_key_value("  0.9999:", "{:.4f} ms".format(alias["resp_perc"]["0.9999"]*1000)))
                right.append(self.format_key_value("  0.999:", "{:.4f} ms".format(alias["resp_perc"]["0.999"]*1000)))
                right.append(self.format_key_value("  0.99:", "{:.4f} ms".format(alias["resp_perc"]["0.99"]*1000)))
                right.append(self.format_key_value("  0.9:", "{:.4f} ms".format(alias["resp_perc"]["0.9"]*1000)))
                right.append(self.format_key_value("  0.5 (median):", "{:.4f} ms".format(alias["resp_perc"]["0.5"]*1000)))
                
                for z in range(0, max(len(left), len(right))):
                    if z not in left: left.append("")
                    if z not in right: right.append("")
                    text += "{:<50} {:<30}\n".format(left[z], right[z])

                if (i+1) < len(stats[stage]["operations"]):
                    text += '\n'

            box_text(text)

    def get_all_basic_stats(self):
        stats = {}
        self.stages = self.get_stages()
        for stage in self.stages:
            if stage in self.not_workload: continue
            stage_range = self.get_stage_range(stage)
            stats[stage] = self.get_stats(stage, stage_range[0], stage_range[1])
        return stats

    def get_all_csv(self):
        stages = self.get_stages()
        for stage in stages:
            stage_range = self.get_stage_range(stage)
            series = self.get_series(stage, stage_range[0], stage_range[1])
            print(series)


    def get_tick(self, timestamp, quanta=STATS_QUANTA):
        return int(timestamp)-(int(timestamp) % quanta)


    def get_series(self, stage, start, stop):
        workers = self.get_workers()
        cur = self.conn.cursor()
        series_master = {}
        start_s = start/1000
        stop_s = stop/1000

        for i in range(self.get_tick(start_s), self.get_tick(stop_s)+STATS_QUANTA, STATS_QUANTA):
            series_master[i] = {"global": {}}

        for i, worker in enumerate(workers):
            query = "SELECT data,ts FROM {} WHERE stage=\"{}\" AND ts >= {} AND ts <= {} ORDER BY ts".format(worker, stage, start, stop)
            data = cur.execute(query)

            for row in data:
                stat = json.loads(row[0])
                if "final" in stat["value"]: continue
                tick = self.get_tick(row[1]/1000)
                
                for op in stat["value"]["st"]:
                    if op not in series_master[tick]:
                        series_master[tick][op] = {
                            "bytes/s": 0,
                            "ios/s": 0,
                            "resp": 0,
                            "iotime": 0,
                            "failures": 0
                        }
        

                    series_master[tick][op]["bytes/s"] += stat["value"]["st"][op]["bytes/s"]
                    series_master[tick][op]["ios/s"] += stat["value"]["st"][op]["ios/s"]
                    series_master[tick][op]["iotime"] += stat["value"]["st"][op]["iotime"]
                    series_master[tick][op]["failures"] += stat["value"]["st"][op]["failures"]/stat["value"]["walltime"]
                    
                    if len(stat["value"]["st"][op]["resp"]) == 0: continue
                    
                    resp = 0
                    for r in stat["value"]["st"][op]["resp"]:
                        resp += r
                    series_master[tick][op]["resp"] = resp/len(stat["value"]["st"][op]["resp"])
        
        return series_master

    def get_config(self, stage="global"):

        query = "SELECT * FROM config WHERE stage='{}'".format(stage)
        cur = self.conn.cursor()
        data = cur.execute(query)

        # only 1 row or there's some issue
        retval = json.loads(next(data)[2])
        cur.close()
        return retval


    def get_stats(self, stage, start, stop):
        """
        One giant pass to avoid running the same queries over and over again.
        """
        workers = self.get_workers()
        cur = self.conn.cursor()
        iops = {}
        bytes = {}
        fail = {}
        resp = {}
        iotime = {}
        ops = []
        resp_all = {}
        returnval = {}
        g_wall = 0
        g_iotime = 0
        g_wrkld_eff = 0
        g_ios = 0
        window = (stop - start)/1000
        sample_count = 0
        ##
        # Loop through all of the drivers
        for i, worker in enumerate(workers):
            query = "SELECT data FROM {} WHERE stage=\"{}\" AND ts >= {} AND ts <= {} ORDER BY ts".format(worker, stage, start, stop)
            data = cur.execute(query)

            ##
            # Each row is a sample from a single driver
            for row in data:
                stat = json.loads(row[0])

                # "final" entries are figures for the benchmark
                if "final" in stat["value"]: continue

                g_wall += stat["value"]["walltime"]
                g_iotime += stat["value"]["iotime"]
                g_wrkld_eff += stat["value"]["wrkld_eff"]
                g_ios += stat["value"]["ios"]
                sample_count += 1

                ##
                # Process each operation in the sample
                for op in stat["value"]["st"]:

                    # Maintain a list of all the ops we're tracing
                    if op not in ops:
                        ops.append(op)

                    # Create stats vars for this op
                    if op not in iops:
                        iops[op] = 0
                        bytes[op] = 0
                        resp[op] = 0
                        fail[op] = 0
                        iotime[op] = 0
                        resp_all[op] = []

                    iops[op] += stat["value"]["st"][op]["ios"]
                    bytes[op] += stat["value"]["st"][op]["bytes"]
                    iotime[op] += stat["value"]["st"][op]["iotime"]
                    fail[op] += stat["value"]["st"][op]["failures"]
                    if len(stat["value"]["st"][op]["resp"]) > 0:
                        for r in stat["value"]["st"][op]["resp"]:
                            resp[op] += r
                            resp_all[op].append(r)

        returnval = {
            "global": {
                "window": window,
                "iotime": g_iotime,
                "walltime": g_wall,
                "wrkld_eff": g_wrkld_eff/sample_count,
                "ios": 0
            },
            "operations": {}
        }
        for op in ops:
            returnval["global"]["ios"] += iops[op]
            returnval["operations"][op] = {
                "iops/s": iops[op]/window,
                "bytes/s": bytes[op]/window,
                "failures": fail[op]/window,
                "iotime": iotime[op],
                "resp_avg": resp[op]/len(resp_all[op]),
                "ttl_operations": iops[op],
                "ttl_bytes": bytes[op],
                "resp_perc": self.get_precentiles(resp_all[op])
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
        self.workers=[]

        query = "SELECT name FROM sqlite_master WHERE type='table'"
        cur = self.conn.cursor()
        for x in cur.execute(query):
            if x[0] == "config": continue
            self.workers.append(x[0])
        cur.close()
        
        return(self.workers)


    def get_stages(self):
        if self.stages != None:
            return self.stages
        self.stages = []

        workers = self.get_workers()
        cur = self.conn.cursor()
        for worker in workers:
            for x in cur.execute("SELECT DISTINCT \"stage\" FROM (SELECT stage, ts FROM {} ORDER BY ts)".format(worker)):
                if x[0] not in self.stages: self.stages.append(x[0])
        cur.close()
        return self.stages


    def get_processes(self, stage):
        if stage in self.processes:
            return self.processes[stage]
        self.processes[stage] = []
    
        workers = self.get_workers()
        for worker in workers:
            query = "SELECT DISTINCT t_id FROM {} WHERE stage=\"{}\"".format(worker, stage)
            for x in self.conn.cursor().execute(query):
                self.processes[stage].append((worker, x[0]))
        return self.processes[stage]


    def get_stage_range(self, stage):
        processes = self.get_processes(stage)
        cur = self.conn.cursor()
        min_vals = []
        max_vals = []
        for process in processes:
            query = "SELECT MIN(ts) AS min_ts FROM {} WHERE stage=\"{}\" AND t_id={}".format(process[0], stage, process[1])
            for value in cur.execute(query):
                min_vals.append(value[0])
        min = min_vals[0]
        for v in min_vals:
            if v < min: min = v

        for process in processes:
            query = "SELECT MAX(ts) AS max_ts FROM {} WHERE stage=\"{}\" AND t_id={}".format(process[0], stage, process[1])
            for value in cur.execute(query):
                max_vals.append(value[0])
        max = max_vals[0]
        for v in min_vals:
            if v > max: max = v

        return((min, max))