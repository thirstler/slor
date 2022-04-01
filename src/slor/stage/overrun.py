from slor.shared import *
from slor.process import SlorProcess


class Overrun(SlorProcess):
    def __init__(self, socket, config, w_id, id):
        self.sock = socket
        self.id = id
        self.w_id = w_id
        self.config = config
        self.operations = ("write",)

    def ready(self):

        self.mk_byte_pool(DEFAULT_CACHE_OVERRUN_OBJ * 2)

        if self.hand_shake():
            self.delay()
            self.exec()

    def exec(self):

        count = (
            int(
                self.config["cache_overrun_sz"]
                / self.config["threads"]
                / DEFAULT_CACHE_OVERRUN_OBJ
            )
            + 1
        )
        
        self.start_benchmark(("write",), target=count)
        self.start_sample()

        for o in range(0, count):

            if self.check_for_messages() == "stop":
                break

            body_data = self.get_bytes_from_pool(DEFAULT_CACHE_OVERRUN_OBJ)
            key = "w{}t{}o{}".format(self.config["w_id"], self.id, o)

            for i in range(0, PREPARE_RETRIES):
                try:
                    self.start_io("write")
                    self.s3ops.put_object(
                        "{0}{1}".format(
                            self.config["bucket_prefix"],
                            (o % self.config["bucket_count"]),
                        ),
                        "{0}{1}".format(DEFAULT_CACHE_OVERRUN_PREFIX, key),
                        body_data,
                    )
                    self.stop_io(sz=DEFAULT_CACHE_OVERRUN_OBJ)
                    break  # worked, no need to retry

                except Exception as e:
                    self.stop_io(failed=True)
                    sys.stderr.write("retry[{0}]: {1}\n".format(self.id, str(e)))
                    sys.stderr.flush()
                    self.fail_count += 1
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
