import sqlite3
from shared import *
import os
from pathlib import Path
import json
import time

class SlorDB:

    db_file = None
    db_conn = None
    db_cursor = None
    config = None

    def __init__(self, config):

        self.config = config
        if self.config["no_db"]: return

        dbroot = POSIX_DB_TMP
        if os.name == "nt":
            dbroot = WINDOWS_DB_TMP

        if self.db_conn == None:
            self.db_file = Path("{}{}.db".format(dbroot, self.config["name"]))
            vcount = 1
            while os.path.exists(self.db_file):
                self.db_file = Path("{}{}_{}.db".format(dbroot, self.config["name"], vcount))
                vcount += 1
            self.db_conn = sqlite3.connect(self.db_file.as_posix())
            self.db_cursor = self.db_conn.cursor()
            query = "CREATE TABLE config (ts INT, stage STRING, data JSON)"
            self.db_cursor.execute(query)
            query = "INSERT INTO config VALUES ({}, 'global', '{}')".format(time.time(), json.dumps(config))
            self.db_cursor.execute(query)
            self.db_conn.commit()

    def commit_stage_config(self, stage, config):
        if self.config["no_db"]: return
        query = "INSERT INTO config VALUES ({}, '{}', '{}')".format(time.time(), stage, json.dumps(config))
        self.db_cursor.execute(query)
        self.db_conn.commit()

    def mk_data_store(self, host):
        if self.config["no_db"]: return
        host = self.host_table_hash(host)
        query =  "CREATE TABLE {0} (t_id INT, ts INT, stage STRING, data JSON)".format(host)
        self.db_cursor.execute(query)
        self.db_cursor.execute(
            "CREATE INDEX {0}_ts_i ON {0} (ts)".format(host)
        )
        self.db_conn.commit()

    def store_stat(self, message, stage_itr=None):
        if self.config["no_db"]: return
        host = self.host_table_hash(message["w_id"])
        if stage_itr:
            stage = "{}:{}".format(message["stage"], stage_itr)
        else:
            stage = message["stage"]
        # Add to database for analysis later
        sql = "INSERT INTO {0} VALUES ({1}, {2}, '{3}', '{4}')".format(
            host,
            message["t_id"],
            message["time"],
            stage,
            json.dumps(message),
        )
        self.db_cursor.execute(sql)
        self.db_conn.commit()

    def host_table_hash(self, hostname):
        return "h"+hex(hash(hostname))[2:]