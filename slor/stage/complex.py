

class Mixed(SlorProcess):

    def __init__(self, socket, config, id):
        self.sock = socket
        self.id = id
        self.config = config
        self.operations = ("complex",)
    
    def exec(self):
        pass
