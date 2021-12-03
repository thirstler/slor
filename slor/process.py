import boto3
import sys


class SlorProcess:

    sock = None
    config = None
    id = None
    s3client = None
    stop = False
    timing_data = []

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

    def msg_to_worker(
        self,
        type="message",
        data_type="string",
        value=None,
        key=None,
        label=None,
        time_ms=None,
    ):
        mesg = {"type": type, "data_type": data_type, "value": value, "t_id": self.id}
        if key != None:
            mesg["key"] = key
        if time_ms != None:
            mesg["time"] = time_ms
        if label != None:
            mesg["label"] = label

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
