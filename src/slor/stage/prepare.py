from slor.shared import *
from slor.process import SlorProcess


class Prepare(SlorProcess):

    r1 = None
    r2 = None

    def __init__(self, socket, config, w_id, id):
        self.sock = socket
        self.id = id
        self.w_id = w_id
        self.config = config
        self.operations = ("write",)
        self.rangeObj = sizeRange(low=int(config["sz_range"]["low"]), high=int(config["sz_range"]["high"]))
        

    def ready(self):

        if self.hand_shake():
            if self.config["random_from_pool"]:
                self.mk_byte_pool(self.rangeObj.high * 2)
            self.delay()
            self.exec()

    def get_mapslice(self):
        return(self.mapslice)

    def exec(self):

        self.msg_to_driver(type="driver", value="process started for prepare stage")
        self.start_benchmark(("write",), target=len(self.config["mapslice"]))
        self.start_sample()

        count = 0
        for o, skey in enumerate(self.config["mapslice"]):
            if self.check_for_messages() == "stop":
                break

            #body_data = self.get_bytes_from_pool(self.rangeObj.getVal())
            body_data = self.get_random_bytes(self.rangeObj.getVal(), from_pool=self.config["random_from_pool"], compressible=self.config["compressible"])
            blen = len(body_data)

            # Retry loop. Prepared data PUTS need to be retried until
            # successful rather than failed and logged.
            # Behavior if all retries fail is not defined.
            for i in range(0, PREPARE_RETRIES):

                try:
                    self.start_io("write")
                    
                    # If we specified and MPU size, write objects as MPUs
                    if self.config["mpu_size"]:

                        mpu = self.s3ops.s3client.create_multipart_upload(
                            Bucket=skey[0], Key=skey[1]
                        )
                        mpu_info = []
                        for part_num in range(1, int(blen / self.config["mpu_size"]) + 2):
                            outer = part_num * self.config["mpu_size"]
                            part_bytes = (
                                self.config["mpu_size"]
                                if outer <= blen
                                else (self.config["mpu_size"] - (outer - blen))
                            )
                            #body_data = self.get_bytes_from_pool(int(part_bytes))
                            body_data = self.get_random_bytes(int(part_bytes), from_pool=self.config["random_from_pool"])
                            up_resp = self.s3ops.s3client.upload_part(
                                Body=body_data,
                                Bucket=skey[0],
                                Key=skey[1],
                                PartNumber=part_num,
                                UploadId=mpu["UploadId"],
                            )
                            mpu_info.append({"PartNumber": part_num, "ETag": up_resp["ETag"]})
                            if outer == blen:
                                break
                        self.s3ops.s3client.complete_multipart_upload(
                            Bucket=skey[0],
                            Key=skey[1],
                            UploadId=mpu["UploadId"],
                            MultipartUpload={"Parts": mpu_info},
                        )

                    # Otherwise, just put the damn thing
                    else:
                        resp = self.s3ops.put_object(skey[0], skey[1], body_data)

                    self.stop_io(sz=len(body_data))

                    # Record the version-id. This gets reported back to the
                    # controller
                    if self.config["versioning"]:
                        self.config["mapslice"][o][2].append(resp["VersionId"])
                        
                    count += 1

                    break  # worked, no need to retry
                
                # Not cool, any excpetion will result in a retry. Will revisit
                # one day if necessary.
                except Exception as e:
                    sys.stderr.write("retry[{0}]: {1}\n".format(self.id, str(e)))
                    sys.stderr.flush()
                    continue  # Keep trying, you can do it

            # Report-in every now and then
            if (
                self.unit_start - self.sample_struct.window_start
            ) >= DRIVER_REPORT_TIMER:
                self.stop_sample()
                self.start_sample()

        # wrap it up
        self.stop_sample()
        self.stop_benchmark()
        
        # Relay new readmap w/versions back to the controller
        self.msg_to_driver(value=self.config["mapslice"], type="readmap")
