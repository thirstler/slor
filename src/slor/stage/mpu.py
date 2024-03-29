from slor.shared import *
from slor.process import SlorProcess
import random
import time


class Mpu(SlorProcess):
    
    def __init__(self, socket, config, w_id, id):
        self.sock = socket
        self.id = id
        self.w_id = w_id
        self.config = config
        self.operations = ("write",)
        self.benchmark_stop = time.time() + config["run_time"]
        self.rangeObj = sizeRange(low=self.config["sz_range"]["low"], high=self.config["sz_range"]["high"])

    def ready(self):

        if self.hand_shake():
            if self.config["random_from_pool"]:
                self.mk_byte_pool(self.rangeObj.high * 2)
            self.delay()
            self.exec()

    def exec(self):
        self.msg_to_driver(type="driver", value="process started for write (MPU) stage")
        w_str = str(self.config["w_id"])
        self.start_benchmark()
        self.start_sample()
        ocount = 0
        while True:
            bucket = "{}{}".format(
                self.config["bucket_prefix"],
                str(int(random.random() * self.config["bucket_count"])),
            )
            key = gen_key(
                (self.config["key_sz"]["low"], self.config["key_sz"]["high"]),
                inc=ocount,
                prefix=DEFAULT_WRITE_PREFIX + self.config["key_prefix"] + w_str,
            )
            blen = self.rangeObj.getVal()

            try:
                self.start_io("write")
                mpu = self.s3ops.s3client.create_multipart_upload(
                    Bucket=bucket, Key=key
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
                        Bucket=bucket,
                        Key=key,
                        PartNumber=part_num,
                        UploadId=mpu["UploadId"],
                    )
                    mpu_info.append({"PartNumber": part_num, "ETag": up_resp["ETag"]})
                    if outer == blen:
                        break
                self.s3ops.s3client.complete_multipart_upload(
                    Bucket=bucket,
                    Key=key,
                    UploadId=mpu["UploadId"],
                    MultipartUpload={"Parts": mpu_info},
                )
                self.stop_io(sz=blen)
                ocount += 1

            except Exception as e:
                sys.stderr.write(
                    "fail[{0}] {0}/{1}: {2}\n".format(self.id, bucket, key, str(e))
                )
                sys.stderr.flush()
                self.stop_io(failed=True)

            if self.unit_start >= self.benchmark_stop:
                self.stop_sample()
                self.stop_benchmark()
                break

            elif (
                self.unit_start - self.sample_struct.window_start
            ) >= DRIVER_REPORT_TIMER:
                self.stop_sample()
                self.start_sample()
