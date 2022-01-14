import boto3

class S3primitives:

    def __init__(self, config=None):
        if config:
            self.set_s3_client(config)

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