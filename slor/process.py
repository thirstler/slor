import boto3
import sys
import time

class SlorProcess:

    sock = None
    config = None
    id = None             # Process ID (unique to the worker, not the whole distributed job)
    s3client = None
    stop = False

    nownow = 0            # Current time
    fail_count = 0
    benchmark_start = 0   # benchmark start time
    benchmark_iops = 0    # operations per second for this bench (total average)
    benchmark_count = 0   # I/O count, total for benchmark
    benchmark_stop = 0    # when this benchmark is _supposed_ to stop
    sample_start = 0      # reporting sample start time
    sample_iops = 0       # operations per second for this sample
    sample_count = 0      # I/O count for reporting sample
    sample_content_ln = 0 # body content length (for bandwidth calc)
    sample_bandwidth = 0  # bandwidth figures
    sample_ttc = []       # array of time-to-complete I/O timings
    unit_start = 0        # start of individual I/O 
    unit_time = 0         # time to complete I/O

    def start_benchmark(self):
        self.benchmark_start = time.time()
        self.benchmark_iops = 0
        self.benchmark_count = 0
        self.fail_count = 0
        self.benchmark_stop = self.benchmark_start + self.config["run_time"]

    def stop_benchmark(self):
        self.benchmark_iops = float(self.benchmark_count) / (time.time() - self.benchmark_start)
        #self.benchmark_iops = 0
        #self.benchmark_count = 0

    def inc_io_count(self):
        self.benchmark_count += 1
        self.sample_count += 1

    def inc_content_len(self, length):
        self.sample_content_ln += length

    def start_sample(self):
        self.sample_start = time.time()
        self.sample_iops = 0
        self.sample_count = 0
        self.sample_ttc.clear()
        self.sample_content_ln = 0

    def stop_sample(self):
        now = time.time()
        self.sample_iops = float(self.sample_count) / (now - self.sample_start)
        self.sample_bandwidth = float(self.sample_content_ln) / (now - self.sample_start)
        #self.benchmark_iops = float(self.benchmark_count) / (now - self.benchmark_start)

    def start_io(self):
        self.unit_start = time.time()
    
    def stop_io(self):
        self.unit_time = time.time() - self.unit_start
        self.sample_ttc.append(self.unit_time)
        self.inc_io_count()

    def check_for_messages(self):

        if self.sock.poll():
            msg = self.sock.recv()
            if "command" in msg and msg["command"] == "stop":
                self.stop = True
                return "stop"

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

    def assess_per_sec(self, td):
        count = 0
        t_time = 0.0
        for count, val in enumerate(td):
            t_time += val["finish"] - val["start"]
        return (count + 1) / t_time


    def log_stats(self, final=False):

        stats = {
            "count": self.benchmark_count,
            "iops": self.sample_iops,
            "failures": self.fail_count,
            "resp": self.sample_ttc,
        }
        if final:
            stats["final"] = True
            stats["benchmark_iops"] = self.benchmark_iops

        self.msg_to_worker(
            type="stat",
            stage=self.config["type"],
            value=stats,
            time_ms=int(self.nownow * 1000)
        )

    def msg_to_worker(
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
            sys.stderr.write("lost contact with main worker (process exiting)\n")
            self.sock.close()
            sys.exit(1)
        except Exception as e:
            sys.stderr.write("error in process: {0} (thread exiting)\n".format(e))
            self.sock.close()
            sys.exit(1)

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
