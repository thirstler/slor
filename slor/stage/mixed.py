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
        for x in self.config["mixed_profile"]:
            self.operations += (x,)
            

    def _read(self):
        if self.all_is_fair:
            key = self.get_key_from_existing()
        else:
            key = self.config["readmap"][self.readmap_index]
        
        
        try:
            self.start_io("read")
            resp = self.s3ops.get_object(key[0], key[1])
            self.stop_io(sz=resp["ContentLength"])
        except Exception as e:
            sys.stderr.write(str(e))
            sys.stderr.flush()
            self.stop_io(failed=True)

        self.readmap_index += 1

        if self.readmap_index >= len(self.config["mapslice"]):
            self.all_is_fair = True
            self.readmap_index = 0

        return resp


    def _write(self):
        size = random.randint(self.config["sz_range"][0], self.config["sz_range"][1])
        body_data = self.get_bytes_from_pool(size)
        self.writemap.append(
            ("{}{}".format(self.config["bucket_prefix"], random.randint(0, self.config["bucket_count"]-1)),
             self.config["key_prefix"] + gen_key(key_desc=self.config["key_sz"], prefix=DEFAULT_WRITE_PREFIX))
        )
        try:
            self.start_io("write")
            self.s3ops.put_object(self.writemap[-1][0], self.writemap[-1][1], body_data)
            self.stop_io(sz=size)
        except Exception as e:
            sys.stderr.write(str(e))
            sys.stderr.flush()
            self.stop_io(failed=True)

    def _head(self):
        hat = self.get_key_from_existing()

        try:
            self.start_io("head")
            self.s3ops.head_object(hat[0], hat[1])
            self.stop_io()
        except Exception as e:
            sys.stderr.write(str(e))
            sys.stderr.flush()
            self.stop_io(failed=True)

    def _delete(self):
        """ Only delete from the written pool """
        if len(self.writemap) == 0:
            return

        indx = random.randint(0, len(self.writemap))
        try:
            
            key = self.writemap.pop(indx)
        except Exception as e:
            sys.stderr.write("{}:{} - {}".format(len(self.writemap), indx,  e))
            return

        try:
            self.start_io("delete")
            self.s3ops.delete_object(key[0], key[1])
            self.stop_io()
        except Exception as e:
            sys.stderr.write(str(e))
            sys.stderr.flush()
            self.stop_io(failed=True)

    def _reread(self):
        """ Only reread from the written pool """
        if len(self.writemap) == 0:
            return

        indx = random.randint(0, len(self.writemap)-1)
        key = self.writemap[indx]

        try:
            self.start_io("reread")
            resp = self.s3ops.get_object(key[0], key[1])
            self.stop_io(sz=resp["ContentLength"])
        except Exception as e:
            sys.stderr.write(str(e))
            sys.stderr.flush()
            self.stop_io(failed=True)

    def _overwrite(self):
        body_data = self.get_bytes_from_pool(
            random.randint(self.config["sz_range"][0], self.config["sz_range"][1]))
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
        """ Fetch a key from both the prepaired data or anything written during the load run"""
        rm_len = len(self.config["mapslice"])
        wn_len = len(self.writemap)
        indx = random.randint(0, rm_len+wn_len-1)
        return self.config["mapslice"][indx] if indx < rm_len else self.writemap[indx-rm_len]


    def mk_dice(self):
        """ Just build a dumb map """
        dice = []
        for m in self.config["mixed_profile"]:
            for x in range(0, self.config["mixed_profile"][m]):
                dice.append(m)
        return dice


    def exec(self):

        dice = self.mk_dice()
        self.mk_byte_pool(WRITE_STAGE_BYTEPOOL_SZ)
        self.start_benchmark()
        self.start_sample()
        while True:

            self.do(dice[random.randint(0,99)])
            
            if self.unit_start >= self.benchmark_stop:
                self.stop_sample()
                self.stop_benchmark()
                break

            elif (self.unit_start - self.sample_struct["start"]) >= DRIVER_REPORT_TIMER:
                self.stop_sample()
                self.start_sample()
