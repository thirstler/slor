from shared import *
from process import SlorProcess
import time
import random

class Overrun(SlorProcess):

    def __init__(self, socket, config, id):
        self.sock = socket
        self.id = id
        self.config = config


    def exec(self):

        count = int(self.config["cache_overrun_sz"]/self.config["threads"]/DEFAULT_CACHE_OVERRUN_OBJ)+1
        print(count)
        self.set_s3_client(self.config)
        self.mk_byte_pool(DEFAULT_CACHE_OVERRUN_OBJ*2)
        self.start_benchmark()
        self.start_sample()

        for o in range(0, count):

            if self.check_for_messages() == "stop":
                break
            
            body_data = self.get_bytes_from_pool(DEFAULT_CACHE_OVERRUN_OBJ)

            for i in range(0, PREPARE_RETRIES):

                try:
                    self.start_io()
                    self.put_object(
                        "{0}{1}".format(self.config["bucket_prefix"], (o % self.config["bucket_count"])),
                        "{0}{1}".format(DEFAULT_CACHE_OVERRUN_PREFIX, o),
                        body_data)
                    self.stop_io()
                    self.inc_content_len(DEFAULT_CACHE_OVERRUN_OBJ)
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