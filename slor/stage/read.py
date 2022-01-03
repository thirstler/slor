from shared import *
from process import SlorProcess

class Read(SlorProcess):

    def __init__(self, socket, config, id):
        self.sock = socket
        self.id = id
        self.config = config

    def exec(self):

        self.set_s3_client(self.config)
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
                    self.start_io()
                    resp = self.get_object(pkey[0], pkey[1])
                    self.stop_io()
                    self.inc_content_len(resp["ContentLength"])

                except Exception as e:
                    sys.stderr.write("fail[{0}] {0}/{1}: {2}\n".format(self.id, pkey[0], pkey[1], str(e)))
                    sys.stderr.flush()
                    self.stop_io(failed=True)
                
                if self.unit_start >= self.benchmark_stop:
                    self.stop_sample()
                    self.stop_benchmark()
                    self.log_stats(final=True)
                    stop = True # break outer loop
                    break

                elif (self.unit_start - self.sample_start) >= WORKER_REPORT_TIMER:
                    self.stop_sample()
                    self.log_stats()
                    self.start_sample()

            rerun += 1