from slor.shared import *
from slor.s3primitives import S3primitives
from slor.sample import perfSample
from numpy.random import Generator
from randomgen import AESCounter
import sys
import time
import random
import json


class SlorProcess:
    """
    Root for all worker processes. Contains shared routines for triggering 
    timers, communicating with the parent (driver) process and getting random
    data for write operations.
    """
    sock = None
    config = None
    id = None  # Process ID (unique to the driver, not the whole distributed job)
    w_id = None  # hostname used to identify the driver
    s3client = None
    stop = False
    byte_pool = b''
    s3ops = None

    benchmark_stop = 0  # when this benchmark is _supposed_ to stop
    unit_start = 0  # start of individual I/O
    sample_struct = None
    default_op = None
    current_op = None
    operations = ()  # Operation types in this workload
    sample_count = 0
    benchark_io_count = 0

    def delay(self):
        """calculate and execute the start-up delay for this process"""
        process_delay = (self.config["startup_delay"] * self.config["w_id"]) + (
            (self.config["startup_delay"] / self.config["threads"]) * self.id
        )
        time.sleep(process_delay)

    ##
    # Benchmark timing functions
    def start_benchmark(self, ops=None, target=None) -> None:
        self.rg = Generator(AESCounter(None))
        self.s3ops = S3primitives(self.config)
        self.count_target = target

    def stop_benchmark(self) -> None:
        pass

    def start_sample(self) -> None:
        self.sample_struct = perfSample(
            driver_id=self.w_id,
            process_id=self.id,
            count_target=self.count_target,
            start_io_count=self.benchark_io_count,
            sample_seq=self.sample_count,
        )
        for o in self.operations:
            self.sample_struct.add_operation_class(o)
        self.sample_count += 1
        self.sample_struct.start()

    def stop_sample(self) -> None:
        self.sample_struct.stop()
        self.sample_struct.ttl = self.sample_struct.window_end + (
            DRIVER_REPORT_TIMER + 1
        )
        self.send_sample(json.loads(self.sample_struct.dump_json()))
        self.benchark_io_count += self.sample_struct.get_workload_ios()
        del self.sample_struct

    def start_io(self, type) -> None:
        self.current_op = type
        self.unit_start = time.time()

    def stop_io(self, failed=False, sz=0, final=False) -> None:

        unit_time = time.time() - self.unit_start
        if failed:
            self.sample_struct.add_failures(self.current_op, 1)
        else:
            self.sample_struct.update(
                opclass=self.current_op, ios=1, bytes=sz, resp_t=unit_time
            )
        self.sample_struct.final = final

    ##
    # Random-data handlers
    def get_random_bytes(self, num_bytes:int, from_pool=False, compressible:float=0.0) -> bytearray:
        '''
        Get N bytes of random bytes from either a pre-created pool of random
        bytes (from_pool=True) or from a PRNG which here should use the
        processor's AES extensions to efficiently make pseudorandom garbage 
        on-the-fly (at the expense of more CPU use)

        Compressible data is made possible by simply shoving a single block of
        zeros in the middle of generated objects. This works to make data 
        compressible but will not make compression algorithms work hard.
        '''
        if from_pool:
            byte_arr = self.get_bytes_from_pool(num_bytes)
        else:
            byte_arr = self.get_bytes_from_prng(num_bytes, compressible=compressible)

        return byte_arr

    def get_bytes_from_prng(self, num_bytes:int, compressible:float=0.0) -> bytearray:

        byte_arr = b''

        if compressible > 0.0:
            zeros = int(num_bytes * compressible)
            num_rnd = num_bytes - zeros
            byte_arr = self.rg.bytes(int(num_rnd/2)) + (b'0'*zeros) + self.rg.bytes(int(num_rnd/2))
            if len(byte_arr) < num_bytes:
                byte_arr += self.rg.bytes(num_bytes-len(byte_arr))
        else:
            byte_arr = self.rg.bytes(num_bytes)

        return byte_arr


    def get_bytes_from_pool(self, num_bytes) -> bytearray:
        '''
        Grabs N bytes of data from the random pool. Grabs at a random offset
        that will wrap-around as needed.
        '''
        if num_bytes == 0: return b''
        start = random.randrange(0, self.pool_sz)
        ext = start + num_bytes
        if ext > self.pool_sz:
            return (
                self.byte_pool[: (ext - self.pool_sz)]
                + self.byte_pool[start : self.pool_sz]
            )
        else:
            return self.byte_pool[start:ext]
    
    def mk_byte_pool(self, num_bytes) -> None:
        bpc = time.time()
        self.byte_pool = (lambda n: bytearray(map(random.getrandbits, (8,) * n)))(
            num_bytes
        )
        self.pool_sz = num_bytes
        bpc = int( (time.time() - bpc) * 1000)
        self.msg_to_driver(type="driver", value="created byte pool in {} ms".format(bpc))

    ##
    # IPC
    def send_sample(self, message):
        self.msg_to_driver(
            type="stat",
            stage=self.config["label"],
            value=message,
            time_ms=int(time.time() * 1000),
        )

    def hand_shake(self):
        self.sock.send({"ready": True})
        mesg = self.sock.recv()
        if mesg["exec"]:
            return True
        return False

    def check_for_messages(self):
        if self.sock.poll():
            msg = self.sock.recv()
            if "command" in msg and msg["command"] == "stop":
                self.stop = True
                return "stop"

    def msg_to_driver(
        self,
        type="message",
        value=None,
        stage=None,
        time_ms=None,
    ):
        mesg = {"type": type, "value": value, "t_id": self.id}
        if stage != None:
            mesg["stage"] = stage
        if time_ms != None:
            mesg["time"] = time_ms

        try:
            self.sock.send(mesg)
        except BrokenPipeError:
            sys.stderr.write("lost contact with main driver (process exiting)\n")
            self.sock.close()
            sys.exit(1)
        except Exception as e:
            sys.stderr.write("error in process: {0} (thread exiting)\n".format(e))
            self.sock.close()
            sys.exit(1)
