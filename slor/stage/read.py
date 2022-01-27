from shared import *
from process import SlorProcess

class Read(SlorProcess):

    def __init__(self, socket, config, id):
        self.sock = socket
        self.id = id
        self.config = config
        self.operations = ("read",)

    def exec(self):

        self.start_benchmark()
        self.start_sample()
        stop = False
        rerun = 0

        # Wrap-around when out of keys to read
        while True:

            if stop: break

            if rerun > 0:
                sys.stderr.write(
                    "WARNING: rereading objects (x{0}), consider increasing iop-limit\n".format(
                        rerun
                    )
                )

            for i, pkey in enumerate(self.config["mapslice"]):

                try:
                    self.start_io("read")
                    resp = self.s3ops.get_object(pkey[0], pkey[1])
                    if resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
                        self.stop_io(failed=True)
                    else:
                        self.stop_io(sz=resp["ContentLength"])

                except Exception as e:
                    self.stop_io(failed=True)
                
                if self.unit_start >= self.benchmark_stop:
                    self.stop_sample()
                    self.stop_benchmark()
                    stop = True # break outer loop
                    break

                elif (self.unit_start - self.sample_struct["start"]) >= DRIVER_REPORT_TIMER:
                    self.stop_sample()
                    self.start_sample()

            rerun += 1