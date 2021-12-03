import argparse
from multiprocessing import Process, Array, Pipe
from multiprocessing.connection import Client, Listener
import time
import uuid
from shared import *
import signal
import boto3
import random
from stages import *
import sys


def _worker_t(socket, config, id):
    """
    Wrapper function to launch workload processes from a Process() call.
    """
    if config["type"] == "prepare":
        wc = SlorPrepare(socket, config, id).exec()
    elif config["type"] == "read":
        wc = SlorRead(socket, config, id).exec()
    elif config["type"] == "write":
        wc = SlorWrite(socket, config, id).exec()
    elif config["type"] == "readwrite":
        wc = SlorReadWrite(socket, config, id).exec()
    elif config["type"] == "delete":
        wc = SlorDelete(socket, config, id).exec()
    elif config["type"] == "metadata_read":
        wc = SlorMetadataRead(socket, config, id).exec()
    elif config["type"] == "metadata_write":
        wc = SlorMetadataWrite(socket, config, id).exec()
    elif config["type"] == "metadata_write":
        wc = SlorMetadataMixed(socket, config, id).exec()


class SlorWorkerHandle:
    """
    Slor worker root class. This
    """

    sock = None
    resp = None
    readmap = []
    procs = []
    pipes = []
    master_messages = None
    sysinf = None
    stop = False
    reset = False

    def __init__(self, socket):

        self.sock = socket

    def exec(self):

        self.sysinf = basic_sysinfo()

        while True:
            if self.reset:
                break

            while self.sock.poll():

                try:
                    cmd_buffer = self.sock.recv()
                except:
                    self.reset = True
                    break

                # Everything should be commands at this point
                if "command" in cmd_buffer:
                    self.decider(cmd_buffer)
                else:
                    self.sock.send({"message": "missing command"})

            time.sleep(0.01)

        # loops and whatever else are done. Close shop.
        self.reset = False
        print("done with controller")
        self.sock.close()

    def init_buckets(self, config):
        """
        Create bucket in our list
        """
        if config["verify"] == True:
            verify_tls = True
        elif config["verify"].to_lower() == "false":
            verify_tls = False
        else:
            verify_tls = config["verify"]
        
        if config["endpoint"][-13:] == "amazonaws.com":
            client = boto3.Session(
                aws_access_key_id=config["access_key"],
                aws_secret_access_key=config["secret_key"],
                region_name=config["region"],
            ).client("s3", verify=verify_tls)
            for bn in config["bucket_list"]:
                try:
                    client.head_bucket(Bucket=bn)
                    self.log_to_controller("Warning: bucket present ({0})".format(bn))
                except:
                    client.create_bucket(Bucket=bn)

        else:
            client = boto3.Session(
                aws_access_key_id=config["access_key"],
                aws_secret_access_key=config["secret_key"],
                region_name=config["region"],
            ).client("s3", verify=verify_tls, endpoint_url=config["endpoint"])

            for bn in config["bucket_list"]:
                try:
                    client.head_bucket(Bucket=bn)
                    self.log_to_controller("Warning: bucket present ({0})".format(bn))
                except:
                    self.log_to_controller("creating {0}".format(bn))
                    client.create_bucket(
                        Bucket=bn,
                        CreateBucketConfiguration={
                            "LocationConstraint": config["region"]
                        },
                    )

    def log_to_controller(self, message):
        """If the message is a string then it will be echoed to the console on the controller"""
        if type(message) is str:
            message = {"message": message}
        message["w_id"] = self.sysinf["uname"].node

        try:
            self.sock.send(message)
        except Exception as e:
            print(message)
            self.reset = True  # lost contact with controller need to close-up

    def thread_control(self, config):

        keys_per_thread = int(
            ((config["prepare_sz"] / config["sz_range"][2]) + 1) / config["threads"]
        )

        ##
        # Receive work
        for id in range(0, config["threads"]):

            if "readmap" in config:

                # Divide the readmap here if we're using one
                offset = id * keys_per_thread
                end = offset + keys_per_thread
                config["mapslice"] = config["readmap"][
                    offset : end if end < len(config["readmap"]) else -1
                ]

            ##
            # Create socket for talking to thread and launch
            self.pipes.append((Pipe()))
            self.procs.append(
                Process(target=_worker_t, args=(self.pipes[-1][1], config, id))
            )
            self.procs[-1].start()

        ##
        # Monitoring and retrun the responces
        while True:
            running = False

            # scan for messages
            for t in self.pipes:

                if self.reset:
                    t[0].send({"command": "stop"})

                while t[0].poll(0.01):

                    # Basically everything is sent back to the controller
                    resp = t[0].recv()
                    resp = self.process_thread_resp(resp)
                    if resp:
                        self.log_to_controller(resp)

            # Mark active if any threads are active
            for t in self.procs:
                if t.is_alive():
                    running = True

            # Batch of threads are exited, break
            if running == False:
                break

            # time.sleep(0.01)

        # Make sure everyone is done (redundant)
        for n in self.procs:
            n.join()

        # Alert controller that the current workload is finished
        self.log_to_controller({"status": "done"})

    def process_thread_resp(self, resp):
        # we can filter messages intended for the driver if we want
        if "status" in resp and resp["status"] == "done":
            print("thread {0} exited".format(resp["t_id"]))
            return False
        return resp

    def decider(self, cmd_buffer):
        """I'm the decider"""

        # Take care of items relevent to the root worker
        if cmd_buffer["command"] == "sysinfo":
            inf_msg = self.sysinf
            inf_msg["status"] = "done"
            self.sock.send(inf_msg)
            return

        # Workloads be here
        elif cmd_buffer["command"] == "workload":

            # Init is done at the worker level, make buckets and exit
            if (
                "type" in cmd_buffer["config"]
                and cmd_buffer["config"]["type"] == "init"
            ):
                self.init_buckets(cmd_buffer["config"])
                self.log_to_controller({"status": "done"})
                return

            # Everything else is managed in separate processes
            self.thread_control(cmd_buffer["config"])


def _slor_worker(bindaddr, bindport):

    """Non-cli entry point"""

    server_sock = Listener((bindaddr, int(bindport)))

    while True:
        # There will only ever be one connection, no connection handling
        sock = server_sock.accept()
        print("new connection")
        handle = SlorWorkerHandle(sock)
        handle.exec()


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
