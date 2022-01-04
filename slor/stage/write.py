from shared import *
from process import SlorProcess
import random

class Write(SlorProcess):

    def __init__(self, socket, config, id):
        self.sock = socket
        self.id = id
        self.config = config

    def exec(self):

        self.mk_byte_pool(WRITE_STAGE_BYTEPOOL_SZ)

        self.set_s3_client(self.config)
        self.start_benchmark()
        self.start_sample()
        while True:
            bucket = "{}{}".format(
                self.config["bucket_prefix"],
                str(self.benchmark_count % self.config["bucket_count"]))
            key  = gen_key(self.config["key_sz"], prefix=DEFAULT_WRITE_PREFIX)
            body = self.get_bytes_from_pool(
                random.randint(self.config["sz_range"][0], self.config["sz_range"][1]))
            try:
                self.start_io()
                self.put_object(bucket, key, body)
                self.stop_io()
                self.inc_content_len(len(body))

            except Exception as e:
                sys.stderr.write("fail[{0}] {0}/{1}: {2}\n".format(self.id, bucket, key, str(e)))
                sys.stderr.flush()
                self.stop_io(failed=True)
            
            if self.unit_start >= self.benchmark_stop:
                self.stop_sample()
                self.stop_benchmark()
                self.log_stats(final=True)
                break

            elif (self.unit_start - self.sample_start) >= DRIVER_REPORT_TIMER:
                self.stop_sample()
                self.log_stats()
                self.start_sample()
        
