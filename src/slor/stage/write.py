from slor.shared import *
from slor.process import SlorProcess
import random
import time


class Write(SlorProcess):
    def __init__(self, socket, config, w_id, id):
        self.sock = socket
        self.id = id
        self.w_id = w_id
        self.config = config
        self.operations = ("write",)
        self.benchmark_stop = time.time() + config["run_time"]

    def ready(self):

        self.mk_byte_pool(int(self.config["sz_range"][1]) * 2)
        if self.hand_shake():
            self.delay()
            self.exec()

    def exec(self):
        w_str = str(self.config["w_id"])
        self.start_benchmark()
        self.start_sample()
        ocount = 0
        while True:
            bucket = "{}{}".format(
                self.config["bucket_prefix"],
                str(int(random.random() * self.config["bucket_count"])),
            )
            key = gen_key(
                self.config["key_sz"],
                inc=ocount,
                prefix=DEFAULT_WRITE_PREFIX + self.config["key_prefix"] + w_str,
            )
            blen = random.randint(
                self.config["sz_range"][0], self.config["sz_range"][1]
            )
            body_data = self.get_bytes_from_pool(blen)
            #try:
            self.start_io("write")
            self.s3ops.put_object(bucket, key, body_data)
            self.stop_io(sz=len(body_data))
            ocount += 1
            #except Exception as e:
            #    sys.stderr.write(
            #        "fail[{0}] {0}/{1}: {2}\n".format(self.id, bucket, key, str(e))
            #    )
            #    sys.stderr.flush()
            #    self.stop_io(failed=True)

            if self.unit_start >= self.benchmark_stop:
                self.stop_sample()
                self.stop_benchmark()
                break

            elif (
                self.unit_start - self.sample_struct.window_start
            ) >= DRIVER_REPORT_TIMER:
                self.stop_sample()
                self.start_sample()
