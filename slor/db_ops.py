import sqlite3
from shared import *
import os
from pathlib import Path
import json

class SlorDB:

    db_file = None
    db_conn = None
    db_cursor = None
    config = None

    def __init__(self, config):
        self.config = config
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

    def mk_data_store(self, host):
        for x in ".-":
            host = host.replace(x, "_")
        self.db_cursor.execute(
            "CREATE TABLE {0} (t_id INT, ts INT, stage STRING, data JSON)".format(
                host
            )
        )
        self.db_cursor.execute(
            "CREATE INDEX {0}_ts_i ON {0} (ts)".format(host)
        )
    
    def store_stat(self, message):
        for x in ".-":
            message["w_id"] = message["w_id"].replace(x, "_")
        
        # Add to database for analysis later
        sql = "INSERT INTO {0} VALUES ({1}, {2}, '{3}', '{4}')".format(
            message["w_id"],
            message["t_id"],
            message["time"],
            message["stage"],
            json.dumps(message),
        )
        self.db_cursor.execute(sql)
        self.db_conn.commit()