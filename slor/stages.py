from process import SlorProcess
from shared import *
import random
import time
import os

class SlorWrite(SlorProcess):

    def __init__(self, socket, config, id):
        self.sock = socket
        self.id = id
        self.config = config

    def exec(self):
        pass


class SlorReadWrite(SlorProcess):
    def __init__(self, socket, config, id):
        self.sock = socket
        self.id = id
        self.config = config

    def exec(self):
        pass


class SlorDelete(SlorProcess):
    def __init__(self, socket, config, id):
        self.sock = socket
        self.id = id
        self.config = config

    def exec(self):
        pass


class SlorMetadataRead(SlorProcess):
    def __init__(self, socket, config, id):
        self.sock = socket
        self.id = id
        self.config = config

    def exec(self):
        pass


class SlorMetadataWrite(SlorProcess):
    def __init__(self, socket, config, id):
        self.sock = socket
        self.id = id
        self.config = config

    def exec(self):
        pass


class SlorMetadataMixed(SlorProcess):
    def __init__(self, socket, config, id):
        self.sock = socket
        self.id = id
        self.config = config

    def exec(self):
        pass

