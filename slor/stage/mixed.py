from shared import *
from process import SlorProcess

class Mixed(SlorProcess):

    writemap = [] # tracker for all things written and deleted
    readmap_index = 0
    all_is_fair = False

    def __init__(self, socket, config, id):
        self.sock = socket
        self.id = id
        self.config = config

    def _read(self):
        if self.all_is_fair:
            key = self.get_key_from_existing()
        else:
            key = self.config["readmap"][self.readmap_index]

        resp = self.get_object(key[0], key[1])
        self.readmap_index += 1

        if self.readmap_index >= len(self.config["mapslice"]):
            self.all_is_fair = True
            self.readmap_index = 0

        return resp


    def _write(self):
        body_data = self.get_bytes_from_pool(
            random.randint(self.config["sz_range"][0], self.config["sz_range"][1]))
        self.writemap.append(
            ("{}{}".format(self.config["bucket_prefix"], random.randint(0, self.config["bucket_count"]-1)),
            gen_key(keyc_desc=self.config["key_sz"], prefix=DEFAULT_WRITE_PREFIX))
        )
        self.put_object(
            "{}{}".format(self.writemap[-1][0], self.writemap[-1][1], body_data)
        )
        return len(body_data)


    def _head(self):
        hat = self.get_key_from_existing()
        self.head_object(hat[0], hat[1])


    def _delete(self):
        """ Only delete from the written pool """
        if len(self.writemap) == 0:
            raise KeyError("nothing to delete")

        indx = random.randint(0, len(self.writemap-1))
        key = self.writemap[indx]
        self.delete_object(key[0], key[1])
        self.writemap.remove(indx)


    def _reread(self):
        """ Only reread from the written pool """
        if len(self.writemap) == 0:
            raise KeyError("nothing to read")

        indx = random.randint(0, len(self.writemap-1))
        key = self.writemap[indx]
        self.get_object(key[0], key[1])


    def _overwrite(self):
        body_data = self.get_bytes_from_pool(
            random.randint(self.config["sz_range"][0], self.config["sz_range"][1]))
        key = self.get_key_from_existing()
        self.put_object(
            "{}{}".format(key[0], key[1], body_data)
        )


    def do(self, operation):
        ret = {}
        if operation == "read":
            ret = self._read()

        elif operation == "write":
            ret["ContentLength"] = self._write()
             
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
        """ Fetch a key from both the prepaired data or anything written during the load run"""
        rm_len = len(self.config["mapslice"])
        wn_len = len(self.writemap)
        indx = random.randint(0, rm_len+wn_len-1)
        return self.config["mapslice"][indx] if indx < rm_len else self.writemap[indx-rm_len]


    def mk_dice(self):
        offset = 0
        dice = {}
        for m in MIXED_LOAD_TYPES:
            if m in self.config["mixed_profile"]:
                upper = offset + int(self.config["mixed_profile"][m])
                dice[upper] = m
                offset += upper

        return dice

    def roll(self, dice):
        roll = random.randint(0,100)
        for m in dice:
            if m < roll: return dice[m]


    def exec(self):

        dice = self.mk_dice()
        self.mk_byte_pool(WRITE_STAGE_BYTEPOOL_SZ)
        self.set_s3_client(self.config)

        self.start_benchmark()
        self.start_sample()
        while True:

            #try:
            self.start_io()
            op = self.roll(dice)
            resp = self.do(op)
            self.stop_io()
            
            if any(x == op for x in ("read", "write", "reread", "overwite")):
                self.inc_content_len(resp["ContentLength"])

            #except Exception as e:
            #    sys.stderr.write(str(e))
            #    sys.stderr.flush()
            #    self.stop_io(failed=True)
            
            if self.unit_start >= self.benchmark_stop:
                self.stop_sample()
                self.stop_benchmark()
                self.log_stats(final=True)
                break

            elif (self.unit_start - self.sample_start) >= DRIVER_REPORT_TIMER:
                self.stop_sample()
                self.log_stats()
                self.start_sample()
