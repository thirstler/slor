from slor.shared import *
from slor.process import SlorProcess
import boto3


class CleanUp(SlorProcess):
    
    def __init__(self, socket, config, w_id, id):
        self.sock = socket
        self.id = id
        self.w_id = w_id
        self.config = config
        self.operations = ("cleanup",)

    def ready(self):

        if self.hand_shake():
            self.delay()
            self.exec()

    def exec(self):

        if self.config["verify"] == True:
            verify_tls = True
        elif self.config["verify"].to_lower() == "false":
            verify_tls = False
        else:
            verify_tls = self.config["verify"]

        s3client = boto3.Session(
            aws_access_key_id=self.config["access_key"],
            aws_secret_access_key=self.config["secret_key"],
            region_name=self.config["region"],
        ).client("s3", verify=verify_tls, endpoint_url=self.config["endpoint"])

        self.start_benchmark()
        self.start_sample()

        try:
            ls_pg = s3client.get_paginator("list_object_versions")
        except:
            ls_pg = s3client.get_paginator("list_objects_v2")

        page_iterator = ls_pg.paginate(Bucket=self.config["bucket"], MaxKeys=1000)
        for page in page_iterator:
            all = {"Objects": []}
            if "Contents" in page:
                for x in page["Contents"]:
                    all["Objects"].append({"Key": x["Key"]})
            if "Versions" in page:
                for x in page["Versions"]:
                    all["Objects"].append(
                        {"Key": x["Key"], "VersionId": x["VersionId"]}
                    )
            if "DeleteMarkers" in page:
                for x in page["DeleteMarkers"]:
                    all["Objects"].append(
                        {"Key": x["Key"], "VersionId": x["VersionId"]}
                    )

            try:
                self.start_io("cleanup")
                resp = s3client.delete_objects(Bucket=self.config["bucket"], Delete=all)
                self.stop_io()

            except Exception as e:
                sys.stderr.write("fail: {}\n".format(str(e)))
                sys.stderr.flush()
                self.stop_io(failed=True)

            if (
                self.unit_start - self.sample_struct.window_start
            ) >= DRIVER_REPORT_TIMER:
                self.stop_sample()
                self.start_sample()

        self.stop_sample()
        if self.config["remove_buckets"]:
            s3client.delete_bucket(Bucket=self.config["bucket"])
        self.stop_benchmark()
