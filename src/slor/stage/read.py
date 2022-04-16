from slor.shared import *
from slor.process import SlorProcess
import time


class Read(SlorProcess):
    def __init__(self, socket, config, w_id, id):
        self.sock = socket
        self.id = id
        self.w_id = w_id
        self.config = config
        self.operations = ("read",)
        self.benchmark_stop = time.time() + config["run_time"]

    def ready(self):

        if self.hand_shake():
            self.delay()
            self.exec()

    def exec(self):

        self.start_benchmark()
        self.start_sample()
        stop = False
        rerun = 0
        rangeref = self.config["get_range"]
        # Wrap-around when out of keys to read
        while True:
            if stop:
                break

            if rerun > 0:
                self.msg_to_driver(type="rereadnotice", value=rerun)

            for i, pkey in enumerate(self.config["mapslice"]):

                version_id = None
                range_specifier = None

                # Pick a version if specified
                if self.config["versioning"] and len(pkey) == 3:
                    version_id = random.choice(pkey[2]) # grab any version

                if self.config["get_range"]:
                    if len(self.config["get_range"]) > 1:
                        sz = random.randint(self.config["get_range"]["low"], self.config["get_range"]["high"])
                    else:
                        sz = self.config["get_range"]["low"]
                    offset = random.randint(0, (self.config["sz_range"]["low"]-sz) )
                    end = offset+sz
                    range_specifier = "bytes={}-{}".format(int(offset), int(end))

                try:
                    self.start_io("read")
                    resp = self.s3ops.get_object(pkey[0], pkey[1], version_id=version_id, range=range_specifier)
                    data = resp["Body"].read() # read streamed data
                    self.stop_io(sz=int(resp["ContentLength"]))
                    del data

                except Exception as e:
                    sys.stderr.write(str(e) + "\n")
                    self.stop_io(failed=True)

                if self.unit_start >= self.benchmark_stop:
                    self.stop_sample()
                    self.stop_benchmark()
                    stop = True  # break outer loop
                    break

                elif (
                    self.unit_start - self.sample_struct.window_start
                ) >= DRIVER_REPORT_TIMER:
                    self.stop_sample()
                    self.start_sample()

            rerun += 1
