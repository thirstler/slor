from slor.shared import *
from slor.process import SlorProcess
import random


class Prepare(SlorProcess):

    r1 = None
    r2 = None

    def __init__(self, socket, config, w_id, id):
        self.sock = socket
        self.id = id
        self.w_id = w_id
        self.config = config
        self.operations = ("write",)

    def ready(self):

        sz_range = self.config["sz_range"]
        self.r1 = int(sz_range[0])
        self.r2 = int(sz_range[1])
        self.mk_byte_pool(int(sz_range[1]) * 2)

        if self.hand_shake():
            self.delay()
            self.exec()

    def get_mapslice(self):
        return(self.mapslice)

    def exec(self):

        self.start_benchmark(("write",), target=len(self.config["mapslice"]))
        self.start_sample()
        count = 0
        for i, skey in enumerate(self.config["mapslice"]):
            if self.check_for_messages() == "stop":
                break

            c_len = random.randint(self.r1, self.r2)
            body_data = self.get_bytes_from_pool(c_len)

            for i in range(0, PREPARE_RETRIES):

                try:
                    self.start_io("write")
                    resp = self.s3ops.put_object(skey[0], skey[1], body_data)
                    self.stop_io(sz=len(body_data))
                    if self.config["versioning"]:
                        if len(skey) == 2:
                            self.config["mapslice"][i] += ([],)
                        self.config["mapslice"][i][2].append(resp["VersionId"])
                        
                    count += 1
                    break  # worked, no need to retry

                except Exception as e:
                    self.stop_io(failed=True)
                    sys.stderr.write("retry[{0}]: {1}\n".format(self.id, str(e)))
                    sys.stderr.flush()
                    continue  # Keep trying, you can do it

            # Report-in every now and then
            if (
                self.unit_start - self.sample_struct.window_start
            ) >= DRIVER_REPORT_TIMER:
                self.stop_sample()
                self.start_sample()

        # wrap it up
        self.stop_sample()
        self.stop_benchmark()
