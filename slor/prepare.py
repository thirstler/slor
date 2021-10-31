
from slor_thread import SlorThread
from shared import *
import random
import time
class SlorPrepare(SlorThread):

    def __init__(self, socket, config, id):
        self.sock = socket
        self.id = id
        self.config = config
        try:
            self.config["prepare_sz"] = (self.config["prepare_sz"] / self.config["threads"]) + 1
        except:
            pass  # whatever

    def exec(self):

        self.set_s3_client(self.config)
        szrange = self.config["sz_range"]
        is_rand_range = False if szrange[0] == szrange[1] else True
        tm = time.time()
        count = 0
        maplen = len(self.config["mapslice"])
        for p in self.config["mapslice"]:
            body_data = random.randbytes(random.randint(int(szrange[0]),int(szrange[1]))) if is_rand_range else random.randbytes(int(szrange[0]))
            self.put_object(p[0], p[1], body_data)
            count += 1
            if (time.time() - tm) >= WORKER_REPORT_TIMER:
                self.msg_to_worker(type="status", data_type="percent", value=(count/maplen), label="precent complete")
                tm = time.time()