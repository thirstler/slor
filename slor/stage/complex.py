

class Mixed(SlorProcess):

    def __init__(self, socket, config, id):
        self.sock = socket
        self.id = id
        self.config = config
        self.operations = ("complex",)
    
    def ready(self):

        self.dice = self.mk_dice()
        self.mk_byte_pool(WRITE_STAGE_BYTEPOOL_SZ)

        if self.hand_shake():
            self.delay()
            self.exec()
        
    def exec(self):
        pass
