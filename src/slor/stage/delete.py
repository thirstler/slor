from slor.shared import *
from slor.process import SlorProcess
import time


class Delete(SlorProcess):

    s3ops = None

    def __init__(self, socket, config, w_id, id):
        self.sock = socket
        self.id = id
        self.w_id = w_id
        self.config = config
        self.operations = ("delete",)
        self.benchmark_stop = time.time() + config["run_time"]

    def ready(self):

        if self.hand_shake():
            self.delay()
            self.exec()

    def exec(self):

        self.msg_to_driver(type="driver", value="process started for delete stage")
        self.start_benchmark()
        self.start_sample()

        # Once done processing the prepared list, you're done.
        for pkey in self.config["mapslice"]:

            version_id = None

            # Pick a version if specificed
            if self.config["versioning"] and len(pkey) == 3:
                version_id = random.choice(pkey[2]) # grab any version

            try:
                self.start_io("delete")
                self.s3ops.delete_object(pkey[0], pkey[1], version_id=version_id)
                self.stop_io()

            except Exception as e:
                sys.stderr.write(
                    "fail[{0}] {1}/{2}: {3}\n".format(self.id, pkey[0], pkey[1], str(e))
                )
                sys.stderr.flush()
                self.stop_io(failed=True)

            if self.unit_start >= self.benchmark_stop:
                self.stop_sample()
                self.stop_benchmark()
                break

            elif (
                self.unit_start - self.sample_struct.window_start
            ) >= DRIVER_REPORT_TIMER:
                self.stop_sample()
                self.start_sample()
