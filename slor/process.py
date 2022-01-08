import boto3
import sys
import time
import random
from shared import *
import struct

class SlorProcess:

    sock = None
    config = None
    id = None                # Process ID (unique to the driver, not the whole distributed job)
    s3client = None
    stop = False
    byte_pool = 0

    fail_count = 0
    benchmark_stop = 0       # when this benchmark is _supposed_ to stop
    sample_ttc = []          # array of time-to-complete I/O timings
    unit_start = 0           # start of individual I/O 
    unit_time = 0            # time to complete I/O
    sample_struct = None
    benchmark_struct = None      
    default_op = None
    current_op = None
    operations = ()          # Operation types in this workload


    def new_sample(self, start=None):
        sample = {
            "start": start if start else 0,
            "end": 0,
            "st": {}
        }
        for t in self.operations:

            sample["st"][t] = {
                "resp": [],
                "bytes": 0,
                "ios": 0,
                "failures": 0,
                "iotime": 0
            }
        return sample

    
    ##
    # Benchmark timing functions
    def start_benchmark(self, operations):
        self.benchmark_struct = self.new_sample(time.time())
        self.benchmark_struct["final"] = True
        self.benchmark_stop = self.benchmark_struct["start"] + self.config["run_time"]
        self.operations = operations
        
    def stop_benchmark(self):
        self.benchmark_struct["end"] = time.time()
        self.send_sample(final=True)

    def start_sample(self):
        self.sample_struct = self.new_sample(start=time.time())

    def stop_sample(self):
        self.sample_struct["end"] = time.time()
        self.send_sample()

        for o in self.operations:
            self.benchmark_struct["st"][o]["bytes"] += self.sample_struct["st"][o]["bytes"]
            self.benchmark_struct["st"][o]["iotime"] += self.sample_struct["st"][o]["iotime"]
            self.benchmark_struct["st"][o]["ios"] += self.sample_struct["st"][o]["ios"]
            self.benchmark_struct["st"][o]["failures"] += self.sample_struct["st"][o]["failures"]

    def start_io(self, type):
        self.current_op = type
        self.unit_time = 0
        self.unit_start = time.time()

    def stop_io(self, failed=False, sz=None):
        self.unit_time = time.time() - self.unit_start
        if failed:
            print("############ {} ############".format(self.current_op))
            self.sample_struct["st"][self.current_op]["failures"] += 1
            self.sample_struct["st"][self.current_op]["iotime"] += self.unit_time
        else:
            self.sample_struct["st"][self.current_op]["resp"].append(struct.pack("f", self.unit_time))
            self.sample_struct["st"][self.current_op]["ios"] += 1
            self.sample_struct["st"][self.current_op]["iotime"] += self.unit_time
            if sz:
                self.sample_struct["st"][self.current_op]["bytes"] += sz
                
    def send_sample(self, final=False):

        if final:
            message = self.benchmark_struct
        else:
            message = self.sample_struct

        self.msg_to_driver(
            type="stat",
            stage=self.config["type"],
            value=message,
            time_ms=int(time.time() * 1000)
        )


    ##
    # Random data handlers
    def get_bytes_from_pool(self, num_bytes) -> bytearray:
        start = random.randrange(0, self.pool_sz)
        ext = start + num_bytes
        if ext > self.pool_sz:
            return self.byte_pool[:(ext-self.pool_sz)] + self.byte_pool[start:self.pool_sz]
        else:
            return self.byte_pool[start:ext]

    def mk_byte_pool(self, num_bytes) -> None:
        self.byte_pool = (lambda n:bytearray(map(random.getrandbits,(8,)*n)))(num_bytes)
        self.pool_sz = num_bytes


    ##
    # IPC
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


    ##
    # S3 operation primitives
    def set_s3_client(self, config):

        if config["verify"] == True:
            verify_tls = True
        elif config["verify"].to_lower() == "false":
            verify_tls = False
        else:
            verify_tls = config["verify"]

        # Hopefully we can replace this with a lower-level client. For now, Boto3.
        self.s3client = boto3.Session(
            aws_access_key_id=config["access_key"],
            aws_secret_access_key=config["secret_key"],
            region_name=config["region"],
        ).client("s3", verify=verify_tls, endpoint_url=config["endpoint"])

    def put_object(self, bucket, key, data):
        resp = self.s3client.put_object(
            Bucket=bucket,
            Key=key,
            Body=data,
        )
        return resp

    def get_object(self, bucket, key, version_id=None):
        if version_id != None:
            resp = self.s3client.get_object(
                Bucket=bucket, Key=key, VersionId=version_id
            )
        else:
            resp = self.s3client.get_object(Bucket=bucket, Key=key)

        return resp

    def head_object(self, bucket, key, version_id=None):
        if version_id != None:
            resp = self.s3client.head_object(
                Bucket=bucket, Key=key, VersionId=version_id
            )
        else:
            resp = self.s3client.head_object(
                Bucket=bucket, Key=key
            )
        return resp

    def delete_object(self, bucket, key, version_id=None):
        if version_id != None:
            resp = self.s3client.delete_object(
                Bucket=bucket, Key=key, VersionId=version_id
            )
        else:
            resp = self.s3client.delete_object(
                Bucket=bucket, Key=key
            )
        return resp