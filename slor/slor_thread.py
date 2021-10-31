import boto3

class SlorThread:

    sock = None
    config = None
    id = None
    s3client = None

    def set_s3_client(self, config):

        if config["verify"] == True: verify_tls=True
        elif config["verify"].to_lower() == 'false':  verify_tls=False
        else: verify_tls=config["verify"]

        # Hopefully we can replace this with a lower-level client. For now, Boto3.
        self.s3client =  boto3.Session(
            aws_access_key_id=config["access_key"],
            aws_secret_access_key=config["secret_key"],
            region_name=config["region"]
        ).client("s3", verify=verify_tls, endpoint_url=config["endpoint"])

    def msg_to_worker(self, type="message", data_type="string", value=None, label=None):
        mesg = {
            "type": type,
            "data_type": data_type,
            "value": value,
            "t_id": self.id
        }
        if label != None: mesg["label"] = label

        self.sock.send(mesg)

    def put_object(self, bucket, key, data):

        self.s3client.put_object(
                Bucket=bucket,
                Key=key,
                Body=data,
            )