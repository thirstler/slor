

class Mixed(SlorProcess):

    def __init__(self, socket, config, id):
        self.sock = socket
        self.id = id
        self.config = config
        self.operations = ("complex",)
    
    def ready(self):

        self.dice = self.mk_dice()
        self.mk_byte_pool(WRITE_STAGE_BYTEPOOL_SZ)

        ##
        # Boiler-place
        self.sock.send({"ready": True})
        mesg = self.sock.recv()
        if mesg["exec"]:
            self.exec()
        else:
            return False
        return True
        
    def exec(self):
        pass
