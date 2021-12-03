from process import SlorProcess
from shared import *
import random
import time
import uuid


class SlorRead(SlorProcess):
    def __init__(self, socket, config, id):
        self.sock = socket
        self.id = id
        self.config = config

    def exec(self):

        self.set_s3_client(self.config)

        start = tm = time.time()
        stop = start + self.config["run_time"]
        count_ttl = 0
        rerun = 0

        while True:

            if tm >= stop: break

            for i, pkey in enumerate(self.config["mapslice"]):

                ##
                # Execute the GET and gather timing 
                td = {"t_id": self.id, "start": time.time()}
                try:
                    data = self.get_object(pkey[0], pkey[1])
                    td["status"] = "success"
                except Exception as e:
                    # No need to retry, log failure and move on
                    td["status"] = "failed"
                    sys.stderr.write(str(e))
                tm = td["finish"] = time.time()
                self.timing_data.append(td)
                count_ttl += 1

                ##
                # Log the GETs if it's time or if out of time
                if (td["finish"] - tm) >= WORKER_REPORT_TIMER or tm >= stop:
                    self.msg_to_worker(
                        type="stat",
                        key="read",
                        data_type="timers",
                        value=self.timing_data,
                        time_ms=int(td["finish"] * 1000),
                    )
                    self.timing_data.clear()
                    if tm >= stop: break


            if rerun > 0:
                self.msg_to_worker(
                    value="WARNING: rereading objects (x{0}), consider increasing iop-limit".format(
                        rerun
                    )
                )
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
        count_ttl = 0
        rerun = 0

        junk = random.randbytes(int(szrange[1]))
        
        while True:

            if tm >= stop: break

            body_data = (
                junk[:random.randint(int(szrange[0]), int(szrange[1]))]
                if is_rand_range
                else junk
            )

            ##
            # Execute the PUT and gather timing 
            td = {"t_id": self.id, "start": time.time()}
            try:
                data = self.put_object(
                    "{0}{1}".format(
                        self.config["bucket_prefix"],
                        random.randint(0, (self.config["bucket_count"]-1))
                    ),
                    "{0}/{1}".format(DEFAULT_WRITE_PREFIX, str(uuid.uuid4())),
                    body_data,
                )
                td["status"] = "success"
            except Exception as e:
                td["status"] = "failed"
                sys.stderr.write("{0}\n".format(str(e)))
            tm = td["finish"] = time.time()
            self.timing_data.append(td)
            count_ttl += 1

            ##
            # Log the PUTs if it's time or if out of time
            if (td["finish"] - tm) >= WORKER_REPORT_TIMER or tm >= stop:
                self.msg_to_worker(
                    type="stat",
                    key="write",
                    data_type="timers",
                    value=self.timing_data,
                    time_ms=int(td["finish"] * 1000),
                )
                self.timing_data.clear()
                if tm >= stop: break


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
        tm = time.time()
        maplen = self.config["mapslice"]
        for count, skey in enumerate(self.config["mapslice"]):

            if self.check_for_messages() == "stop":
                break

            nownow = time.time()
            body_data = (
                random.randbytes(random.randint(int(szrange[0]), int(szrange[1])))
                if is_rand_range
                else random.randbytes(int(szrange[0]))
            )

            for i in range(0, PREPARE_RETRIES):
                try:
                    self.put_object(skey[0], skey[1], body_data)
                    break
                except Exception as e:
                    sys.stderr.write("retry: {0}".format(str(e)))
                    continue
                sys.stderr.write("ERROR: object failed to write: {0}/{0}".format(skey[0], skey[1]))
                

            # Report-in every now and then
            if (nownow - tm) >= WORKER_REPORT_TIMER or count == maplen:
                self.msg_to_worker(
                    type="stat",
                    key="prepare",
                    data_type="counter",
                    value=count,
                    time_ms=int(nownow * 1000),
                )
                tm = nownow
