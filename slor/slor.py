#!/usr/bin/python3

import sys
from shared import *

if __name__ == "__main__":

    try:
        sys.argv[1]
    except:
        sys.stderr.write(ROOT_HELP)
        sys.exit(1)

    if sys.argv[1] == "controller":
        import control

        control.run()

    elif sys.argv[1] == "worker":
        import worker

        worker.run()

    else:
        sys.stderr.write(ROOT_HELP)
