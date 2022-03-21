from slor.shared import *
from slor.process import SlorProcess
import time


class Mixed(SlorProcess):

    writemap = []  # tracker for all things written (and deleted)
    readmap_index = 0
    dice = None
    wid_str = None

    def __init__(self, socket, config, w_id, id):
        self.sock = socket
        self.id = id
        self.w_id = w_id
        self.config = config
        for x in self.config["mixed_profile"]:
            self.operations += (x,)
        self.wid_str = str(self.config["w_id"])
        self.benchmark_stop = time.time() + config["run_time"]

    def ready(self):

        self.dice = self.mk_dice()
        self.mk_byte_pool(WRITE_STAGE_BYTEPOOL_SZ)

        if self.hand_shake():
            self.delay()
            self.exec()

    def _read(self):
        resp = None
        version_id = None

        # If we run out of prepared data (we're trying not to reread anything),
        # then just read from any pool of written data. 
        if self.readmap_index >= len(self.config["mapslice"]):
            key = self.get_key_from_existing()
        else:
            key = self.config["readmap"][self.readmap_index]

        # Pick a version if specificed
        if self.config["versioning"] and len(key) == 3:
            version_id = random.choice(key[2]) # grab any version

        try:
            self.start_io("read")
            resp = self.s3ops.get_object(key[0], key[1], version_id=version_id)
            data = resp["Body"].read()
            self.stop_io(sz=resp["ContentLength"])
            del data
        except Exception as e:
            sys.stderr.write("err: {}, {} {} {}\n".format(str(e), key[0], key[1], version_id))
            sys.stderr.flush()
            self.stop_io(failed=True)

        # Increment to then next prepared data key
        self.readmap_index += 1

        return resp

    def _mpu(self):
        """
        perform write as an MPU of size specified in the load definition.
        Uses boto3 directly rather than through a wrapper.
        """
        blen = random.randint(self.config["sz_range"][0], self.config["sz_range"][1])
        self.writemap.append(
            (
                "{}{}".format(
                    self.config["bucket_prefix"],
                    random.randint(0, self.config["bucket_count"] - 1),
                ),
                self.config["key_prefix"]
                + gen_key(
                    key_desc=self.config["key_sz"],
                    inc=len(self.writemap),
                    prefix=DEFAULT_WRITE_PREFIX + self.wid_str,
                ),
            )
        )
        bucket = self.writemap[-1][0]
        key = self.writemap[-1][1]
        try:

            self.start_io("write")
            mpu = self.s3ops.s3client.create_multipart_upload(Bucket=bucket, Key=key)
            mpu_info = []
            for part_num in range(1, int(blen / self.config["mpu_size"]) + 2):
                outer = part_num * self.config["mpu_size"]
                bytes = (
                    self.config["mpu_size"]
                    if outer <= blen
                    else (self.config["mpu_size"] - (outer - blen))
                )
                body_data = self.get_bytes_from_pool(int(bytes))
                up_resp = self.s3ops.s3client.upload_part(
                    Body=body_data,
                    Bucket=bucket,
                    Key=key,
                    PartNumber=part_num,
                    UploadId=mpu["UploadId"],
                )
                mpu_info.append({"PartNumber": part_num, "ETag": up_resp["ETag"]})
                if outer == blen: # size was precisely on MPU boundary
                    break

            resp = self.s3ops.s3client.complete_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=mpu["UploadId"],
                MultipartUpload={"Parts": mpu_info},
            )
            self.stop_io(sz=blen)

            if len(self.writemap[-1]) == 2 and "VersionId" in resp:
                self.writemap[-1].append([])
            self.writemap[-1][2].append(resp["VersionId"])

        except Exception as e:
            sys.stderr.write(str(e))
            sys.stderr.flush()
            self.stop_io(failed=True)

    def _write(self):
        size = random.randint(self.config["sz_range"][0], self.config["sz_range"][1])
        body_data = self.get_bytes_from_pool(size)

        # Create new key and add to list of written objects
        self.writemap.append(
            [
                "{}{}".format(
                    self.config["bucket_prefix"],
                    random.randint(0, self.config["bucket_count"] - 1),
                ),
                self.config["key_prefix"]
                + gen_key(
                    key_desc=self.config["key_sz"],
                    inc=len(self.writemap),
                    prefix=DEFAULT_WRITE_PREFIX + self.wid_str,
                )
            ]
        )
        try:
            self.start_io("write")
            resp = self.s3ops.put_object(self.writemap[-1][0], self.writemap[-1][1], body_data)
            self.stop_io(sz=size)

            # If --versioning is specified, add version information to writemap
            if self.config["versioning"] and "VersionId" in resp:
                if len(self.writemap[-1]) == 2:
                    self.writemap[-1].append([])
                self.writemap[-1][2].append(resp["VersionId"])

        except Exception as e:
            sys.stderr.write("err: {}, {} {}\n".format(str(e), self.writemap[-1][0], self.writemap[-1][1]))
            self.writemap.pop(-1)
            sys.stderr.flush()
            self.stop_io(failed=True)

    def _head(self):

        hat = self.get_key_from_existing()
        version_id = None

        if self.config["versioning"] and len(hat) == 3:
            version_id = random.choice(hat[2]) # grab any version

        try:
            self.start_io("head")
            self.s3ops.head_object(hat[0], hat[1], version_id=version_id)
            self.stop_io()
        except Exception as e:
            sys.stderr.write("{} - {} - {} - {}".format(hat[0], hat[1], version_id, str(e)))
            sys.stderr.flush()
            self.stop_io(failed=True)

    def _delete(self):
        """Only delete from the written pool"""
        if len(self.writemap) == 0:
            # Nothing to delete yet, skip it
            return

        version_id = None
        kindx = random.randint(0, len(self.writemap)-1)
        key = self.writemap[kindx]

        # Remove from writelist before successful delete. Since we don't try
        # to understand what failures is, we have to.
        if self.config["versioning"] and len(key) == 3:
            # Grab version ID and remove from list of versions
            version_id = key[2].pop(-1)
            if len(key[2]) == 0:
                # Or delete whole entry if that was the only version
                del self.writemap[kindx]
        else:
            del self.writemap[kindx]

        try:
            self.start_io("delete")
            self.s3ops.delete_object(key[0], key[1], version_id=version_id)
            self.stop_io()
        except Exception as e:
            sys.stderr.write(str(e))
            sys.stderr.flush()
            self.stop_io(failed=True)

    def _reread(self):
        """Only reread from the written pool"""
        if len(self.writemap) == 0:
            # Nothing to reread yet, skip it
            return

        version_id = None
        indx = random.randint(0, len(self.writemap) - 1)
        key = self.writemap[indx]

        if self.config["versioning"] and len(key) == 3:
            version_id = random.choice(key[2]) # grab any version

        try:
            self.start_io("reread")
            resp = self.s3ops.get_object(key[0], key[1], version_id=version_id)
            self.stop_io(sz=resp["ContentLength"])
        except Exception as e:
            sys.stderr.write("err: {}, {} {} {}\n".format(str(e), key[0], key[1], version_id))
            sys.stderr.flush()
            self.stop_io(failed=True)

    def _overwrite(self):
        body_data = self.get_bytes_from_pool(
            random.randint(self.config["sz_range"][0], self.config["sz_range"][1])
        )
        key = self.get_key_from_existing()
        size = len(body_data)
        
        try:
            self.start_io("overwrite")
            self.s3ops.put_object(key[0], key[1], body_data)
            self.stop_io(sz=size)
        except Exception as e:
            sys.stderr.write(str(e))
            sys.stderr.flush()
            self.stop_io(failed=True)

    def do(self, operation):
        ret = {}
        if operation == "read":
            ret = self._read()

        elif operation == "write":
            if self.config["mpu_size"]:
                self._mpu()
            else:
                self._write()

        elif operation == "head":
            self._head()

        elif operation == "delete":
            self._delete()

        elif operation == "reread":
            self._reread()

        elif operation == "overwrite":
            self._overwrite()

        return ret

    def get_key_from_existing(self):
        """Fetch a key from both the prepaired data or anything written during the load run"""
        rm_len = len(self.config["mapslice"])
        wn_len = len(self.writemap)
        indx = random.randint(0, rm_len + wn_len - 1)
        return (
            self.config["mapslice"][indx]
            if indx < rm_len
            else self.writemap[indx - rm_len]
        )

    def mk_dice(self):

        dice = []
        for m in self.config["mixed_profile"]:
            for x in range(0, self.config["mixed_profile"][m]):
                dice.append(m)
        return dice

    def exec(self):

        self.start_benchmark()
        self.start_sample()
        while True:

            self.do(random.choice(self.dice))

            if self.unit_start >= self.benchmark_stop:
                self.stop_sample()
                self.stop_benchmark()
                break

            elif (
                self.unit_start - self.sample_struct.window_start
            ) >= DRIVER_REPORT_TIMER:
                self.stop_sample()
                self.start_sample()
