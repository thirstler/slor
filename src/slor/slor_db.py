from slor.shared import *
import sqlite3
import os
from pathlib import Path
import pickle
import time
import statistics

def _slor_db(sock, config):
    """ SLorDB entry shim """
    slordb_proc = SlorDB(sock, config)
    slordb_proc.ready()


class SlorDB:
    
    poll = None
    config = None
    resp_t_buffer = None
    prec_sample_len = None
    stats = None
    db_conn = None
    db_cursor = None
    stage_record = None

    def __init__(self, sock, config, stats_sample_len=DEFAULT_STATS_SAMPLE_LEN):
        self.sock = sock
        self.config = config
        self.resp_t_buffer = {}
        self.stats = {}
        self.stage_record = {}

        self.mk_db_conn()

        for host in config["driver_list"]:
            self.mk_data_store("{}:{}".format(host["host"], host["port"]))


    def ready(self):

        while True:
            
            try:
                message = self.sock.recv()
            except:
                # ignore garbage
                continue

            if "command" in message:
                if message["command"] == "STOP":
                    break
            elif "stage_config" in message:
                self.commit_stage_config(message["stage"], message["stage_config"])
            elif "type" in message and message["type"] == "stat":
                self.store_stat(message)


    def mk_data_store(self, host):
        if self.config["no_db"]:
            return
        host = self.host_table_hash(host)
        query = "CREATE TABLE {0} (t_id INT, ts INT, stage STRING, data BLOB)".format(
            host
        )
        self.db_cursor.execute(query)
        self.db_cursor.execute("CREATE INDEX {0}_ts_i ON {0} (ts)".format(host))
        self.db_conn.commit()


    def store_stat(self, message):
        host = self.host_table_hash(message["w_id"])
        stage = "{}:{}".format(message["stage"], self.stage_record[message["stage"]])
        

        # Add to database for analysis later
        sql = "INSERT INTO {0} VALUES ({1}, {2}, '{3}', ?)".format(
            host,
            message["t_id"],
            message["time"],
            stage
        )
        self.db_cursor.execute(sql, [pickle.dumps(message)])
        self.db_conn.commit()


    def host_table_hash(self, hostname):
        return "h" + hex(hash(hostname))[2:]


    def mk_db_conn(self):
        dbroot = POSIX_DB_TMP
        if os.name == "nt":
            dbroot = WINDOWS_DB_TMP

        if self.db_conn == None:
            self.db_file = Path("{}{}.db".format(dbroot, self.config["name"]))

            # Find a file name for the db
            vcount = 1
            while os.path.exists(self.db_file):
                self.db_file = Path(
                    "{}{}_{}.db".format(dbroot, self.config["name"], vcount)
                )
                vcount += 1

            self.db_conn = sqlite3.connect(self.db_file.as_posix())
            self.db_cursor = self.db_conn.cursor()
            query = "CREATE TABLE config (ts INT, stage STRING, data BLOB)"
            self.db_cursor.execute(query)
            query = "INSERT INTO config VALUES ({}, 'global', ?)".format(time.time())
            self.db_cursor.execute(query, [pickle.dumps(self.config)])
            self.db_conn.commit()


    def commit_stage_config(self, stage, config):

        if stage not in self.stage_record:
            self.stage_record[stage] = 1
        else:
            self.stage_record[stage] += 1

        query = "INSERT INTO config VALUES ({}, '{}', ?)".format(
            time.time(), "{}:{}".format(stage,self.stage_record[stage])
        )
        self.db_cursor.execute(query, [pickle.dumps(config)])
        self.db_conn.commit()


    def calc_resp_stats(self) -> None:
        self.resp_t_buffer.sort()
        rb_leb = len(self.resp_t_buffer)
        self.stats = {
            "mean": statistics.mean(self.resp_t_buffer),
            "median": statistics.median(self.resp_t_buffer),
            "min": self.resp_t_buffer[0],
            "max": self.resp_t_buffer[-1],
            "p9999": self.resp_t_buffer[int(0.9999 * rb_leb)],
            "p999": self.resp_t_buffer[int(0.999 * rb_leb)],
            "p99": self.resp_t_buffer[int(0.99 * rb_leb)],
            "p9": self.resp_t_buffer[int(0.9 * rb_leb)]
        }

    def write_stats_to_db(self) -> bool:
        return True

    def logger(self, message:str):
        self.sock.send({"log": {"message": message}})
        



            


    