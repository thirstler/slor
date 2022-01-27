import sqlite3

class SlorAnalysis:
    conn = None
    stages = None
    workers = None
    processes = {}

    def __init__(self, db_file):

        self.conn = sqlite3.connect(db_file)
        self.stages = self.get_stages()
        for stage in self.stages:
            self.get_stage_range(stage)

    def __del__(self):
        self.conn.close()

    def get_workers(self):
        if self.workers != None:
            return self.workers
        self.workers=[]

        query = "SELECT name FROM sqlite_master WHERE type='table'"
        cur = self.conn.cursor()
        for x in cur.execute(query):
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
            for x in cur.execute("SELECT DISTINCT stage FROM (SELECT stage, ts FROM {} ORDER BY ts)".format(worker)):
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

        print("{}: {} - {} = {}".format(stage, min, max, (max - min)))

        #exit()


