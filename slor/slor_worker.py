import sys
import argparse
from multiprocessing import Process, Array, Pipe
from multiprocessing.connection import Client, Listener
import time
import uuid
from shared import *
import signal
import boto3
import random
from prepare import SlorPrepare

# wraper function for starting worker processes
def worker_t(socket, config, id):
    """
    Multi-processed entry
    """
    if config["type"] == "prepare":
        wc = SlorPrepare(socket, config, id).exec()


class SlorWorkerHandle:

    sock = None
    cmd_buffer = None
    resp = None
    readmap = []
    procs = []
    pipes = []
    master_messages = None
    sysinf = None

    def __init__(self, socket):

        self.sock = socket
        signal.signal(signal.SIGINT, self.exit)

    def exit(self):
        self.sock.close()
        exit(0)

    def exec(self):

        self.sysinf = basic_sysinfo()

        while True:

            while self.sock.poll():

                cmd_buffer = self.sock.recv()

                # Everything should be commands at this point
                if "command" in cmd_buffer:
                    self.decider(cmd_buffer)
                else:
                    self.sock.send({"message": "missing command"})

            time.sleep(0.1)

        # loops and whatever else are done. Close shop.
        self.sock.close()


    def init_buckets(self, config):
        '''
        Not sure this belongs here but only one worker needs to setup buckets
        and no worker threads are necessary to execute it. Loading up boto3
        just for this is annoying, but whatever.
        '''
        retval = True
        if config["verify"] == True: verify_tls=True
        elif config["verify"].to_lower() == 'false':  verify_tls=False
        else: verify_tls=config["verify"]

        client = boto3.Session(
            aws_access_key_id=config["access_key"],
            aws_secret_access_key=config["secret_key"],
            region_name=config["region"]
        ).client("s3", verify=verify_tls, endpoint_url=config["endpoint"])

        for b in range(0, int(config["bucket_count"])):
            bn = "{0}{1}".format(config["bucket_prefix"], b)
            try: 
                client.head_bucket(Bucket=bn)
                print("present: {0}".format(bn))
            except:
                print("creating {0}".format(bn))
                try:
                    client.create_bucket(
                        Bucket=bn,
                        CreateBucketConfiguration={
                            'LocationConstraint': config["region"]
                        }
                    )
                except:
                    retval = False
        return retval


    def mk_read_map(self, config):

        objcount = int(config["prepare_sz"] / config["sz_range"][2]) + 1

        self.log_to_controller("building readmap ({0} objects)".format(objcount))

        for z in range(0, objcount):
            self.readmap.append(("{0}{1}".format(config["bucket_prefix"], random.randrange(0, config["bucket_count"])), "{0}/{1}".format(OBJECT_PREFIX_LOC, str(uuid.uuid4()), False)))

        # to make things simple
        self.log_to_controller("done building readmap")

        return config


    def message_handler(self, message):
        message["w_id"] = self.sysinf["uname"].node
        self.sock.send(message)


    def process_control(self, config):

        objcount = int(
            ((config["prepare_sz"] / config["sz_range"][2]) + 1) / config["threads"]
        )

        # Add the readmap to the config
        slice_index = 0
        for id in range(0, config["threads"]):

            # Kind of sloppy but divide the readmap here
            config["mapslice"] = self.readmap[
                slice_index : slice_index + objcount
                if slice_index + objcount < len(self.readmap)
                else -1
            ]
            slice_index += objcount

            self.pipes.append((Pipe()))
            self.procs.append(
                Process(target=worker_t, args=(self.pipes[-1][1], config, id))
            )
            self.procs[-1].start()

        threadstack = config["threads"]
        # Monitoring threads
        while True:

            running = False
            for t in self.procs:
                if t.is_alive():
                    running = True

            # scan for messages
            for t in self.pipes:
                while t[0].poll():
                    self.message_handler(t[0].recv())

            if running == False:
                break

            time.sleep(0.1)

        # Make sure everyone is done
        for n in self.procs:
            n.join()
        print("processes joined")

    def execute_prepare(self, config):

        if len(self.readmap) == 0:
            # Tell controller we're making a readmap
            self.mk_read_map(config)

        self.log_to_controller(
            "preparing {0} bytes of data...".format(
                human_readable(config["prepare_sz"])
            )
        )
        self.log_to_controller("launching {0} threads...".format(config["threads"]))

        self.process_control(config)

    def workload_director(self, workload):

        config = workload["config"]
        if config["type"] == "prepare":
            self.execute_prepare(config)

        return

    def decider(self, cmd_buffer):
        """ I'm the decider """

        # Take care of items relevent to the root worker
        if cmd_buffer["command"] == "sysinfo":
            inf_msg = self.sysinf
            inf_msg["status"] = "done"
            self.sock.send(inf_msg)
            return
        elif cmd_buffer["command"] == "init":
            if self.init_buckets(cmd_buffer["config"]):
                self.sock.send({"message": "bucket(s) initialized", "source": self.sysinf["uname"].node})
            else:
                self.sock.send({"error": True, "message": "failed to initialize buckets", "source": self.sysinf["uname"].node})
        elif cmd_buffer["command"] == "workload":
            self.workload_director(cmd_buffer)

    def log_to_controller(self, message):
        msg = {"message": message, "source": self.sysinf["uname"].node}
        try:
            self.sock.send(msg)
        except:
            sys.stderr.write("couldn't send message: {0}".format(str(msg)))


def _slor_worker(bindaddr, bindport):
    """ Non-cli entry point """
    server_sock = Listener((bindaddr, int(bindport)))

    while True:

        try:
            sock = server_sock.accept()
            handle = SlorWorkerHandle(sock)
            handle.exec()
        except KeyboardInterrupt:
            server_sock.close()
            exit(0)
        except EOFError:
            print("done")


def run():
    parser = argparse.ArgumentParser(
        description="Slor (S3 Load Ruler) is a distributed load generation and benchmarking tool for S3 storage"
    )
    parser.add_argument("worker")  # Make argparse happy
    parser.add_argument(
        "--bindaddr",
        default="0.0.0.0",
        help="bind to specific address (defaults to 0.0.0.0)",
    )
    parser.add_argument(
        "--listen",
        default=DEFAULT_WORKER_PORT,
        help="worker listen port (defaults to {0})".format(DEFAULT_WORKER_PORT),
    )
    args = parser.parse_args()

    _slor_worker(args.bindaddr, args.listen)
