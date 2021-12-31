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
                    self.stop_io()
                    self.inc_content_len(resp["ContentLength"])

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

class SlorBlowout(SlorProcess):

    def __init__(self, socket, config, id):
        self.sock = socket
        self.id = id
        self.config = config

    def exec(self):

        self.set_s3_client(self.config)

        objcount = int(self.config["cache_overrun_sz"]/self.config["threads"]/CACHE_OVERRUN_OBJ_SZ)+1
        self.mk_byte_pool(CACHE_OVERRUN_OBJ_SZ * 2)

        self.start_benchmark()
        self.start_sample()
        for bo in range(0, objcount):

            self.nownow = time.time()  # Does anyone really know what time it is?

            if self.check_for_messages() == "stop":
                break
            
            body_data = self.get_bytes_from_pool(CACHE_OVERRUN_OBJ_SZ)

            for i in range(0, PREPARE_RETRIES):

                try:
                    self.start_io()
                    self.put_object("{0}{1}".format(self.config["bucket_prefix"], bo % self.config["bucket_count"]), str(i), body_data)
                    self.stop_io()
                    self.inc_content_len(CACHE_OVERRUN_OBJ_SZ)
                    break # worked, no need to retry

                except Exception as e:
                    sys.stderr.write("retry: {0}".format(str(e)))
                    self.fail_count += 1
                    continue # Keep trying, you can do it

            if self.benchmark_count == objcount:
                self.stop_sample()
                self.stop_benchmark()
                self.log_stats(final=True)

            # Report-in every now and then
            elif (self.nownow - self.sample_start) >= WORKER_REPORT_TIMER:
                self.stop_sample()
                self.log_stats()
                self.start_sample()
                self.start_sample()


class SlorPrepare(SlorProcess):

    def __init__(self, socket, config, id):
        self.sock = socket
        self.id = id
        self.config = config


    def exec(self):

        self.set_s3_client(self.config)
        sz_range = self.config["sz_range"]
        r1 = int(sz_range[0])
        r2 = int(sz_range[1])
        maplen = len(self.config["mapslice"])

        self.mk_byte_pool(int(sz_range[1]) * 2)

        self.start_benchmark()
        self.start_sample()
        for skey in self.config["mapslice"]:

            self.nownow = time.time()  # Does anyone really know what time it is?

            if self.check_for_messages() == "stop":
                break
            
            c_len = random.randint(r1, r2)
            body_data = self.get_bytes_from_pool(c_len)

            for i in range(0, PREPARE_RETRIES):

                try:
                    self.start_io()
                    self.put_object(skey[0], skey[1], body_data)
                    self.stop_io()
                    self.inc_content_len(c_len)
                    break # worked, no need to retry

                except Exception as e:
                    sys.stderr.write("retry: {0}".format(str(e)))
                    self.fail_count += 1
                    continue # Keep trying, you can do it

            if self.benchmark_count == maplen:
                self.stop_sample()
                self.stop_benchmark()
                self.log_stats(final=True)

            # Report-in every now and then
            elif (self.nownow - self.sample_start) >= WORKER_REPORT_TIMER:
                self.stop_sample()
                self.log_stats()
                self.start_sample()
