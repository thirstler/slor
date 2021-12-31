from shared import *
from process import SlorProcess
import time
import random

class Prepare(SlorProcess):

    def __init__(self, socket, config, id):
        self.sock = socket
        self.id = id
        self.config = config


    def exec(self):

        sz_range = self.config["sz_range"]
        r1 = int(sz_range[0])
        r2 = int(sz_range[1])

        self.set_s3_client(self.config)
        self.mk_byte_pool(int(sz_range[1]) * 2)
        self.start_benchmark()
        self.start_sample()
        
        for skey in self.config["mapslice"]:

            if self.check_for_messages() == "stop":
                break
            
            c_len = random.randint(r1, r2)
            body_data = self.get_bytes_from_pool(c_len)

            for i in range(0, PREPARE_RETRIES):

                try:
                    self.start_io()
                    self.put_object(skey[0], skey[1], body_data)
                    self.stop_io()
                    self.inc_content_len(c_len)
                    break # worked, no need to retry

                except Exception as e:
                    sys.stderr.write("retry[{0}]: {1}\n".format(self.id, str(e)))
                    sys.stderr.flush()
                    self.fail_count += 1
                    continue # Keep trying, you can do it

            # Report-in every now and then
            if (self.unit_start - self.sample_start) >= WORKER_REPORT_TIMER:
                self.stop_sample()
                self.log_stats()
                self.start_sample()

        # wrap it up
        self.stop_sample()
        self.stop_benchmark()
        self.log_stats(final=True)