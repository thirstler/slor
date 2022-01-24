import argparse
from multiprocessing import Process, Array, Pipe
from multiprocessing.connection import Client, Listener
import time
from shared import *
import boto3
from stage import *
import stage.prepare
import stage.read
import stage.overrun
import stage.write
import stage.head
import stage.delete
import stage.mixed
import stage.cleanup
import sys
from s3primitives import S3primitives

def _driver_t(socket, config, id):
    """
    Wrapper function to launch workload processes from a Process() call.
    """
    if config["type"] == "prepare":
        wc = stage.prepare.Prepare(socket, config, id).exec()
    elif config["type"] == "blowout":
        wc = stage.overrun.Overrun(socket, config, id).exec()
    elif config["type"] == "read":
        wc = stage.read.Read(socket, config, id).exec()
    elif config["type"] == "write":
        wc = stage.write.Write(socket, config, id).exec()
    elif config["type"] == "head":
        wc = stage.head.Head(socket, config, id).exec()
    elif config["type"] == "delete":
        wc = stage.delete.Delete(socket, config, id).exec()
    elif config["type"] == "mixed":
        wc = stage.mixed.Mixed(socket, config, id).exec()
    elif config["type"] == "cleanup":
        wc = stage.cleanup.CleanUp(socket, config, id).exec()

    try:
        del wc
    except:
        # whatever
        pass


class SlorDriver:
    """
    Slor driver root class. This
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
    bindaddr = None
    bindport = None
    w_name = None
    worker_name = None

    def __init__(self, socket, bindaddr, bindport):

        self.sock = socket
        self.bindaddr = bindaddr
        self.bindport = bindport

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
        print(" done with controller")
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

        # AWS is entirely dependent on the endpoint to determin region and
        # location constraint.
        if config["endpoint"][-13:] == "amazonaws.com":
            client = boto3.Session(
                aws_access_key_id=config["access_key"],
                aws_secret_access_key=config["secret_key"],
            ).client("s3", verify=verify_tls)
            for bn in config["bucket_list"]:
                try:
                    client.head_bucket(Bucket=bn)
                    self.log_to_controller("Warning: bucket present ({0})".format(bn))
                except:
                    try:
                        client.create_bucket(Bucket=bn)
                    except:
                        self.log_to_controller(
                            "Problem creating bucket: {0}".format(bn)
                        )

        # Generic S3 compatible
        else:
            client = boto3.Session(
                aws_access_key_id=config["access_key"],
                aws_secret_access_key=config["secret_key"],
                region_name=config["region"],
            ).client("s3", verify=verify_tls, endpoint_url=config["endpoint"])

            for bn in config["bucket_list"]:
                try:
                    client.head_bucket(Bucket=bn)
                    #self.log_to_controller("Warning: bucket present ({0})".format(bn))
                except:
                    #self.log_to_controller("creating {0}".format(bn))
                    try:
                        client.create_bucket(
                            Bucket=bn,
                            CreateBucketConfiguration={
                                "LocationConstraint": config["region"]
                            },
                        )
                    except:
                        self.log_to_controller(
                            "Problem creating bucket: {0}".format(bn)
                        )

    def log_to_controller(self, message):
        """If the message is a string then it will be echoed to the console on the controller"""
        if not message:
            return
        if type(message) is str:
            message = {"message": message}
        message["w_id"] = self.worker_name

        try:
            self.sock.send(message)
        except Exception as e:
            #print(message)
            self.reset = True  # lost contact with controller need to close-up

    def thread_control(self, config):
        time.sleep(config["w_id"] * config["startup_delay"])
        delay_time = config["startup_delay"]/config["threads"]

        if config["type"] == "cleanup":
            drivers = len(config["driver_list"])
            buckets = config["bucket_count"]
            who = []
            for n in range(0, drivers):
                who.append([])
            for n in range(0, buckets):
                who[n % drivers].append("{}{}".format(config["bucket_prefix"], n))

            for id, bucket in enumerate(who[config["w_id"]]):
                config["bucket"] = bucket
                self.pipes.append((Pipe()))
                self.procs.append(
                    Process(target=_driver_t, args=(self.pipes[-1][1], config, id))
                )
                self.procs[-1].start()
        else:

            ##
            # Receive work
            for id in range(0, config["threads"]):

                if "readmap" in config:

                    chunk = int(len(config["readmap"])/config["threads"])

                    # Divide the readmap here if we're using one
                    offset = id * chunk
                    end = offset + chunk
                    config["mapslice"] = config["readmap"][offset:end]

                ##
                # Create socket for talking to thread and launch
                self.pipes.append((Pipe()))
                self.procs.append(
                    Process(target=_driver_t, args=(self.pipes[-1][1], config, id))
                )
                time.sleep(delay_time)
                self.procs[-1].start()

        ##
        # Monitoring and return the responses
        while True:
            running = False

            # scan for messages
            for t in self.pipes:

                if self.reset:
                    t[0].send({"command": "stop"})

                while t[0].poll(0.01):

                    # Basically everything is sent back to the controller
                    self.log_to_controller(self.process_thread_resp(t[0].recv()))

            # Mark active if any threads are active
            for t in self.procs:
                if t.is_alive():
                    running = True

            # Batch of threads are exited, break
            if running == False:
                break

        # Make sure everyone is done (redundant)
        for n in self.procs:
            n.join()

        self.procs.clear()
        
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

        # Take care of items relevent to the root driver
        if cmd_buffer["command"] == "sysinfo":
            inf_msg = self.sysinf
            inf_msg["status"] = "done"
            self.sock.send(inf_msg)
            return
        elif cmd_buffer["command"] == "workload":
            if "config" in cmd_buffer:
                self.worker_name = cmd_buffer["config"]["host"]
            # Init is done at the driver level, make buckets and exit
            if (
                "type" in cmd_buffer["config"]
                and cmd_buffer["config"]["type"] == "init"
            ):
                self.init_buckets(cmd_buffer["config"])
                self.log_to_controller({"status": "done"})
                return
                
            # Everything else is managed in separate processes
            self.thread_control(cmd_buffer["config"])


def _slor_driver(bindaddr, bindport, exit_on_disconnect):

    """Non-cli entry point"""
    try:
        server_sock = Listener((bindaddr, int(bindport)))
    except Exception as e:
        print(e)
        sys.exit(1)
        
    print("ready on  {}:{}".format(bindaddr, bindport))
    while True:
        # There will only ever be one connection, no connection handling
        sock = server_sock.accept()
        print(" new connection")
        handle = SlorDriver(sock, bindaddr, bindport)
        handle.exec()
        sock.close()
        del handle
        if exit_on_disconnect:
            return


def run():
    parser = argparse.ArgumentParser(
        description="Slor (S3 Load Ruler) is a distributed load generation and benchmarking tool for S3 storage"
    )
    parser.add_argument("driver")  # Make argparse happy
    parser.add_argument(
        "--bindaddr",
        default="0.0.0.0",
        help="bind to specific address (defaults to 0.0.0.0)"
    )
    parser.add_argument(
        "--listen",
        default=DEFAULT_DRIVER_PORT,
        help="driver listen port (defaults to {0})".format(DEFAULT_DRIVER_PORT),
    )
    args = parser.parse_args()

    _slor_driver(args.bindaddr, args.listen, False)
    
    sys.exit(0)
