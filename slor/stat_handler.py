import json
import sqlite3
import time
import json

class stat_handle():
    
    db_conn = None
    db_cursor = None
    last_print = 0
    timing = 0
    frame = None
    frame_start = None
    last_count = 0

    def __init__(self, db_file, timing):
        self.frame = {}
        self.timing = timing
        try:
            self.db_conn = sqlite3.connect(db_file)
            self.db_cursor = self.db_conn.cursor()
        except:
            print("error accessing db file ({0}) no stats will print".format(db_file))


    def trigger(self):
        nownow = time.time()
        if (nownow - self.last_print) > self.timing:
            self.process()
            self.last_print = nownow
            
    def process(self):
        query = "SELECT name FROM sqlite_master WHERE type='table'"
        then = int((time.time() - self.timing) * 1000)
        for table in self.db_cursor.execute(query).fetchall():
            query = "SELECT key, data, ts FROM {0} WHERE ts > {1}".format(table[0], then)
            for stat in self.db_cursor.execute(query):
                if stat[0] == "prepare": self.proc_prep(stat[1])

        self.print_frame()

    def proc_prep(self, data_json):
        data = json.loads(data_json)
        if data["key"] not in self.frame: self.frame[data["key"]] = {}
        if data["w_id"] not in self.frame[data["key"]]: self.frame[data["key"]][data["w_id"]] = {}
        if data["t_id"] not in self.frame[data["key"]][data["w_id"]]: self.frame[data["key"]][data["w_id"]][data["t_id"]] = {}

        if "time" not in self.frame[data["key"]][data["w_id"]][data["t_id"]]:
            self.frame[data["key"]][data["w_id"]][data["t_id"]] = {"time": data["time"], "count": data["value"]}
        else:
            if self.frame[data["key"]][data["w_id"]][data["t_id"]]["time"] < data["time"]:
                self.frame[data["key"]][data["w_id"]][data["t_id"]] = {"time": data["time"], "count": data["value"]}
        

    def print_frame(self):
        for key in self.frame:
            if key == "prepare":
                count = 0
                for wrkr in self.frame["prepare"]:
                    for thr in self.frame["prepare"][wrkr]:
                        count += self.frame["prepare"][wrkr][thr]["count"]

            print(count)
            self.last_count = count
