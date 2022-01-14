from shared import *
from process import SlorProcess

class Delete(SlorProcess):

    s3ops = None

    def __init__(self, socket, config, id):
        self.sock = socket
        self.id = id
        self.config = config
        self.operations = ("delete",)

    def exec(self):

        self.start_benchmark()
        self.start_sample()
        
        # Once done processing the prepared list, you're done.
        for i, pkey in enumerate(self.config["mapslice"]):

            try:
                self.start_io("delete")
                resp = self.s3ops.delete_object(pkey[0], pkey[1])
                self.stop_io()

            except Exception as e:
                sys.stderr.write("fail[{0}] {1}/{2}: {3}\n".format(self.id, pkey[0], pkey[1], str(e)))
                sys.stderr.flush()
                self.stop_io(failed=True)
            
            if self.unit_start >= self.benchmark_stop:
                self.stop_sample()
                self.stop_benchmark()
                stop = True # break outer loop
                break

            elif (self.unit_start - self.sample_struct["start"]) >= DRIVER_REPORT_TIMER:
                self.stop_sample()
                self.start_sample()