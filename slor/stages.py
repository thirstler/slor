from process import SlorProcess
from shared import *
import random
import time
import os

class SlorRead(SlorProcess):

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
                self.msg_to_worker(
                    value="WARNING: rereading objects (x{0}), consider increasing iop-limit".format(
                        rerun
                    )
                )

            for i, pkey in enumerate(self.config["mapslice"]):

                self.nownow = time.time()
                try:
                    self.start_io()
                    resp = self.get_object(pkey[0], pkey[1])
                    self.inc_content_len(resp["ContentLength"])
                    self.stop_io()

                except Exception as e:
                    sys.stderr.write("retry {0}/{1}: {2}".format(pkey[0], pkey[1], str(e)))
                    self.fail_count += 1
                    continue
                
                if self.nownow >= self.benchmark_stop:
                    self.stop_sample()
                    self.stop_benchmark()
                    self.log_stats(final=True)
                    stop = True
                    break

                elif (self.nownow - self.sample_start) >= WORKER_REPORT_TIMER:

                    self.stop_sample()
                    self.log_stats()
                    self.start_sample()

            rerun += 1


class SlorWrite(SlorProcess):

    def __init__(self, socket, config, id):
        self.sock = socket
        self.id = id
        self.config = config

    def exec(self):

        self.set_s3_client(self.config)
        szrange = self.config["sz_range"]
        is_rand_range = False if szrange[0] == szrange[1] else True

        start = tm = time.time()
        stop = start + self.config["run_time"]

        junk = bytearray(os.urandom(int(szrange[1])) * 2)

        while True:

            if tm >= stop:
                break

            body_data = (
                junk[: random.randint(int(szrange[0]), int(szrange[1]))]
                if is_rand_range
                else junk
            )

            ##
            # Execute the PUT and gather timing
            td = {"start": time.time()}
            try:
                data = self.put_object(
                    "{0}{1}".format(
                        self.config["bucket_prefix"],
                        random.randint(0, (self.config["bucket_count"] - 1)),
                    ),
                    gen_key(
                        key_desc=self.config["key_sz"], prefix=DEFAULT_WRITE_PREFIX
                    ),
                    body_data,
                )
                td["status"] = True
            except Exception as e:
                td["status"] = False
                sys.stderr.write("{0}\n".format(str(e)))
            tm = td["finish"] = time.time()
            self.timing_data.append(td)

            ##
            # Log the PUTs if it's time or if out of time
            if (td["finish"] - tm) >= WORKER_REPORT_TIMER or tm >= stop:
                ops_sec = self.assess_per_sec(self.timing_data)

                self.msg_to_worker(type="stat", stage="write", value=ops_sec, time=tm)
                self.timing_data.clear()
                if tm >= stop:
                    break

    def process(self, td):
        pass


class SlorReadWrite(SlorProcess):
    def __init__(self, socket, config, id):
        self.sock = socket
        self.id = id
        self.config = config

    def exec(self):
        pass


class SlorDelete(SlorProcess):
    def __init__(self, socket, config, id):
        self.sock = socket
        self.id = id
        self.config = config

    def exec(self):
        pass


class SlorMetadataRead(SlorProcess):
    def __init__(self, socket, config, id):
        self.sock = socket
        self.id = id
        self.config = config

    def exec(self):
        pass


class SlorMetadataWrite(SlorProcess):
    def __init__(self, socket, config, id):
        self.sock = socket
        self.id = id
        self.config = config

    def exec(self):
        pass


class SlorMetadataMixed(SlorProcess):
    def __init__(self, socket, config, id):
        self.sock = socket
        self.id = id
        self.config = config

    def exec(self):
        pass


class SlorPrepare(SlorProcess):

    def __init__(self, socket, config, id):
        self.sock = socket
        self.id = id
        self.config = config
        try:
            self.config["prepare_sz"] = (
                self.config["prepare_sz"] / self.config["threads"]
            ) + 1
        except:
            pass  # whatever

    def exec(self):

        self.set_s3_client(self.config)
        szrange = self.config["sz_range"]
        is_rand_range = False if szrange[0] == szrange[1] else True
        maplen = len(self.config["mapslice"])

        # Takes too much effort to generate random data on-the-fly, going to
        # pull random offsets from a pool of bytes
        pool = bytearray(os.urandom(int(szrange[1]) * 2))

        self.start_benchmark()
        self.start_sample()
        for skey in self.config["mapslice"]:

            self.nownow = time.time()  # Does anyone really know what time it is?

            if self.check_for_messages() == "stop":
                break
            
            dsize = random.randint(int(szrange[0]), int(szrange[1]))
            body_data = (
                pool[: dsize ]
                if is_rand_range
                else pool
            )

            for i in range(0, PREPARE_RETRIES):

                try:
                    self.start_io()
                    self.put_object(skey[0], skey[1], body_data)
                    self.inc_content_len(dsize)
                    self.stop_io()
                    break
                except Exception as e:
                    sys.stderr.write("retry: {0}".format(str(e)))
                    self.fail_count += 1
                    continue

            if self.benchmark_count == maplen:
                self.stop_sample()
                self.stop_benchmark()
                self.log_stats(final=True)

            # Report-in every now and then
            elif (self.nownow - self.sample_start) >= WORKER_REPORT_TIMER:
                self.stop_sample()
                self.log_stats()
                self.start_sample()
