import argparse
from shared import *
import sys
from slor_d import SlorDriver
from multiprocessing.connection import Listener

def _slor_driver(bindaddr, bindport, exit_on_disconnect, quiet=False):
    """Non-cli entry point"""
    try:
        server_sock = Listener((bindaddr, int(bindport)))
    except Exception as e:
        print(e)
        sys.exit(1)
        
    if not quiet: print("ready on  {}:{}".format(bindaddr, bindport))
    while True:
        # There will only ever be one connection, no connection handling
        sock = server_sock.accept()
        if not quiet: print(" new connection")
        handle = SlorDriver(sock, bindaddr, bindport)
        handle.exec()
        sock.close()
        del handle
        if exit_on_disconnect:
            return
            

def run():
    """CLI entry point"""
    parser = argparse.ArgumentParser(
        description="Slor (S3 Load Ruler) is a distributed load generation and benchmarking tool for S3 storage"
    )
    parser.add_argument("driver")  # Make argparse happy
    parser.add_argument(
        "--bindaddr",
        default="0.0.0.0",
        help="bind to specific address (defaults to 0.0.0.0)"
    )
    parser.add_argument(
        "--listen",
        default=DEFAULT_DRIVER_PORT,
        help="driver listen port (defaults to {0})".format(DEFAULT_DRIVER_PORT),
    )
    args = parser.parse_args()

    _slor_driver(args.bindaddr, args.listen, False)
    
    sys.exit(0)
