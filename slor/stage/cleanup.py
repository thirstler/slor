from shared import *
from process import SlorProcess

class CleanUp(SlorProcess):

    def __init__(self, socket, config, id):
        self.sock = socket
        self.id = id
        self.config = config
        self.operations = ("cleanup",)

    def exec(self):

        self.start_benchmark()
        self.start_sample()

        for bucket in self.config["bucketlist"]:
            self.s3ops.list_bucket(bucket)

            for page in self.s3ops.page_iterator:

                
                try:
                    self.start_io("cleanup")
                    resp = self.s3ops.delete_page(page)
                    self.stop_io()

                except Exception as e:
                    sys.stderr.write("fail: {)\n".format(str(e)))
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