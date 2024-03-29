from slor.shared import *
import slor.stage.prepare
import slor.stage.read
import slor.stage.overrun
import slor.stage.write
import slor.stage.mpu
import slor.stage.head
import slor.stage.delete
import slor.stage.mixed
import slor.stage.workload
import slor.stage.cleanup
import boto3
import time
import json
from multiprocessing import Process, Pipe


def _driver_t(socket, config, w_id, id):
    """
    Wrapper function to launch workload processes from a Process() call.
    """

    if config["type"] == "prepare":
        slor.stage.prepare.Prepare(socket, config, w_id, id).ready()
    elif config["type"] == "blowout":
        slor.stage.overrun.Overrun(socket, config, w_id, id).ready()
    elif config["type"] == "read":
        slor.stage.read.Read(socket, config, w_id, id).ready()
    elif config["type"] == "write":
        if config["mpu_size"]:
            slor.stage.mpu.Mpu(socket, config, w_id, id).ready()
        else:
            slor.stage.write.Write(socket, config, w_id, id).ready()
    elif config["type"] == "head":
        slor.stage.head.Head(socket, config, w_id, id).ready()
    elif config["type"] == "delete":
        slor.stage.delete.Delete(socket, config, w_id, id).ready()
    elif config["type"] == "mixed":
        slor.stage.mixed.Mixed(socket, config, w_id, id).ready()
    elif config["type"] == "cleanup":
        slor.stage.cleanup.CleanUp(socket, config, w_id, id).ready()


class SlorDriver:
    """
    Slor driver root class.
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
    w_id = None
    logfile = None

    def __init__(self, socket, bindaddr, bindport, args):

        self.sock = socket
        self.bindaddr = bindaddr
        self.bindport = bindport
        self.d_id = str(self.bindaddr)+":"+str(bindport)

        if args != None:
            self.logfile = args.logfile
        else:
            self.logfile = DEFAULT_DRIVER_LOGFILE
        self.logfile += "-"+self.d_id+".log"

        try:
            self.loghandle = open(self.logfile, "a")
        except Exception as e:
            sys.stderr.write("failed to open log file ({}): {}".format(self.logfile, e))
        
        self.logger("driver started")

    def logger(self, message:str, p_id:str=None):
        logobj = {
            "ts": int(time.time() * 1000),
            "d_id": self.d_id,
            "message": message
        }
        if p_id != None:
            logobj["p_id"] = p_id

        try:
            self.loghandle.write(json.dumps(logobj)+"\n")
            self.loghandle.flush()
        except Exception as e:
            sys.stderr.write("failed to write to log file ({}): {}".format(self.logfile, e))

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
        self.logger("done with controller")
        self.sock.close()

    def init_buckets(self, config):
        """
        Create bucket in our list
        Note to self: FIX REDUNDANT SHIT HERE
        """
        if config["verify"] == True:
            verify_tls = True
        elif config["verify"].to_lower() == "false":
            verify_tls = False
        else:
            verify_tls = config["verify"]

        # AWS is dependent on the endpoint to determine region and
        # location constraint.
        if config["endpoint"][-13:] == "amazonaws.com":
            client = boto3.Session(
                aws_access_key_id=config["access_key"],
                aws_secret_access_key=config["secret_key"],
            ).client("s3", verify=verify_tls)
            for bn in config["bucket_list"]:
                try:
                    client.head_bucket(Bucket=bn) # raises an exception if it fails (WTF?)
                    self.log_to_controller("WARNING: bucket present ({0})".format(bn))
                    if not config["use_existing_buckets"]:
                        msg = "ERROR: target buckets exists ({}), exiting for safety (--use-existing if you're sure)\n".format(bn)
                        msg += "\nRemember: if there's a cleanup stage it will DELETE THE ENTIRE BUCKET\n" +\
                            "CONTENTS. For the love of all that is Holy, do not benchmark with an\n" +\
                            "existing bucket that you care about. Just don't.\n"
                        self.log_to_controller({"command": "abort", "message": msg})
                        self.reset = True
                        return

                    if config["versioning"]:
                        resp = client.get_bucket_versioning(
                            Bucket=bn
                        )
                        if not "Status" in resp or resp["Status"] != "Enabled":
                            msg = "WARNING: bucket present ({0}) but versioning not enabled.\n".format(bn) +\
                                "Please delete the bucket or enable versioning on it to proceed (exiting)"
                            self.log_to_controller({"command": "abort", "message": msg})
                            self.reset = True
                            return
                except:
                    try:
                        client.create_bucket(Bucket=bn)
                        if config["versioning"]:
                            client.put_bucket_versioning(
                                Bucket=bn,
                                VersioningConfiguration={'Status': 'Enabled'}
                            )
                    except:
                        self.log_to_controller(
                            "Problem creating bucket: {0}".format(bn)
                        )
                        self.reset = True
                
        # Generic S3 compatible
        else:
            client = boto3.Session(
                aws_access_key_id=config["access_key"],
                aws_secret_access_key=config["secret_key"],
                region_name=config["region"],
            ).client("s3", verify=verify_tls, endpoint_url=config["endpoint"])

            for bn in config["bucket_list"]:
                try:
                    client.head_bucket(Bucket=bn) # raises an exception if it fails (WTF?)
                    self.log_to_controller("WARNING: bucket present ({0})".format(bn))
                    if not config["use_existing_buckets"]:
                        msg = "ERROR: target buckets exists ({}), exiting for safety (--use-existing if you're sure)\n".format(bn)
                        msg += "\nRemember: if there's a cleanup stage it will DELETE THE ENTIRE BUCKET\n" +\
                            "CONTENTS. For the love of all that is Holy, do not benchmark with an\n" +\
                            "existing bucket that you care about. Just don't.\n"
                        self.log_to_controller({"command": "abort", "message": msg})
                        self.reset = True
                        return

                    if config["versioning"]:
                        resp = client.get_bucket_versioning(
                            Bucket=bn
                        )
                        if not "Status" in resp or resp["Status"] != "Enabled":
                            msg = "WARNING: bucket present ({0}) but versioning not enabled.\n".format(bn) +\
                                "Please delete the bucket or enable versioning on it to proceed (exiting)\n"
                            self.log_to_controller({"command": "abort", "message": msg})
                            self.reset = True
                            return
                except:
                    try:
                        client.create_bucket(
                            Bucket=bn,
                            CreateBucketConfiguration={
                                "LocationConstraint": config["region"]
                            },
                        )
                        if config["versioning"]:
                            client.put_bucket_versioning(
                                Bucket=bn,
                                VersioningConfiguration={'Status': 'Enabled'}
                            )
                    except Exception as e:
                        self.log_to_controller(
                            "Problem creating bucket: {0}".format(e)
                        )
                        self.reset = True

    def log_to_controller(self, message):
        """If the message is a string then it will be echoed to the console on the controller"""
        if not message:
            return

        if type(message) is str:
            message = {"message": message}

        message["w_id"] = self.w_id
        try:
            self.sock.send(message)
        except Exception as e:
            # print(message)
            self.reset = True  # lost contact with controller need to close-up

    def check_procs_ready(self):
        ##
        # Check for "ready" status from all launched processes
        global_ready = True
        for t in self.pipes:
            status = t[0].recv()
            if not status["ready"]:
                global_ready = False
        return global_ready

    def report_procs_ready(self):
        self.sock.send({"ready": True})

        # Wait for the go signal
        mesg = self.sock.recv()
        if mesg["exec"] == True:
            for t in self.pipes:
                status = t[0].send({"exec": True})
        return True

    def thread_control(self, config):

        ##
        # Clean-up stage is handled a little differently
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
                    Process(
                        target=_driver_t,
                        args=(self.pipes[-1][1], config, self.w_id, id),
                    )
                )
                self.procs[-1].start()

            if not self.check_procs_ready():
                return False

            if not self.report_procs_ready():
                return False

        else:

            ##
            # Receive work
            for id in range(0, config["threads"]):

                if "readmap" in config:

                    chunk = int(len(config["readmap"]) / config["threads"])

                    # Divide the readmap here if we're using one
                    offset = id * chunk
                    end = offset + chunk
                    config["mapslice"] = config["readmap"][offset:end]

                ##
                # Create socket for talking to thread and launch
                self.pipes.append((Pipe()))
                self.procs.append(
                    Process(
                        target=_driver_t,
                        args=(self.pipes[-1][1], config, self.w_id, id),
                    )
                )
                self.procs[-1].start()

            if not self.check_procs_ready():
                return False
            if not self.report_procs_ready():
                return False

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
                    self.log_to_controller(
                        self.filter_thread_resp(t[0].recv())
                    )

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
        self.pipes.clear()

        # Alert controller that the current workload is finished
        self.log_to_controller({"status": "done"})

    def filter_thread_resp(self, resp):
        # we can filter messages intended for the driver if we want
        if "status" in resp and resp["status"] == "done":
            self.logger("process {0} exited".format(resp["t_id"]))
            return False
        if "type" in resp and resp["type"] == "driver":
            self.logger(resp["value"], p_id=resp["t_id"])
            return False
        
        # pass-thru
        return resp

    def workload_handshake(self):
        self.sock.send({"ready": True})
        mesg = self.sock.recv()
        if mesg["exec"]:
            return True
        return False

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
                self.w_id = "{}:{}".format(cmd_buffer["config"]["host"], cmd_buffer["config"]["port"])
            # Init is done at the driver level, make buckets and exit
            if (
                "type" in cmd_buffer["config"]
                and cmd_buffer["config"]["type"] == "init"
            ):
                if self.workload_handshake():
                    self.init_buckets(cmd_buffer["config"])
                    self.log_to_controller({"status": "done"})
                    return
                return

            # Everything else is managed in separate processes
            self.thread_control(cmd_buffer["config"])
