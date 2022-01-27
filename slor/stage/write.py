from shared import *
from process import SlorProcess
import random

class Write(SlorProcess):

    def __init__(self, socket, config, id):
        self.sock = socket
        self.id = id
        self.config = config
        self.operations = ("write",)

    def exec(self):

        self.mk_byte_pool(WRITE_STAGE_BYTEPOOL_SZ)

        self.start_benchmark()
        self.start_sample()
        while True:
            bucket = "{}{}".format(
                self.config["bucket_prefix"],
                str(int(random.random() * self.config["bucket_count"])))
            key  = self.config["key_prefix"] + gen_key(self.config["key_sz"], prefix=DEFAULT_WRITE_PREFIX)
            blen = random.randint(self.config["sz_range"][0], self.config["sz_range"][1])
            body = self.get_bytes_from_pool(blen)
            try:
                self.start_io("write")
                self.s3ops.put_object(bucket, key, body)
                self.stop_io(sz=blen)

            except Exception as e:
                sys.stderr.write("fail[{0}] {0}/{1}: {2}\n".format(self.id, bucket, key, str(e)))
                sys.stderr.flush()
                self.stop_io(failed=True)
            
            if self.unit_start >= self.benchmark_stop:
                self.stop_sample()
                self.stop_benchmark()
                break

            elif (self.unit_start - self.sample_struct["start"]) >= DRIVER_REPORT_TIMER:
                self.stop_sample()
                self.start_sample()
        
