import boto3


class S3primitives:
    """
    Lame wrapper around boto3 calls just in case one day these should be
    replaced with more manual methods (for better timer placement)
    """

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
        if version_id:
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
            resp = self.s3client.head_object(Bucket=bucket, Key=key)
        return resp

    def delete_object(self, bucket, key, version_id=None):
        if version_id != None:
            resp = self.s3client.delete_object(
                Bucket=bucket, Key=key, VersionId=version_id
            )
        else:
            resp = self.s3client.delete_object(Bucket=bucket, Key=key)
        return resp

    def list_bucket(self, bucket, paged=True):
        lspgntr = None
        if paged:
            lspgntr = self.s3client.get_paginator("list_object_versions")
            self.page_iterator = lspgntr.paginate(Bucket=bucket, MaxKeys=1000)
        else:
            pass  # fuck off

        return self.page_iterator

    def get_listing_page(self) -> iter:
        return self.page_iterator

    def delete_page(self, bucket, page_listing):
        allthings = page_listing["Versions"] + page_listing["DeleteMarkers"]
        deleted = self.s3client.delete_objects(Bucket=bucket, Delete=allthings)
        return deleted

    ##
    # Less primitive
    def delete_recursive(self, bucket, batch_delete=True):
        if batch_delete:
            lspgntr = self.s3client("list_object_versions")
            page_iterator = lspgntr.paginate(Bucket=bucket, MaxKeys=1000)
            for listing in page_iterator:
                versions = listing["Versions"]
                markers = listing["DeleteMarkers"]
                allthings = versions + markers
                deleted = self.s3client.delete_objects(Bucket=bucket, Delete=allthings)
                print(deleted)
        else:
            pass  # fuck it
