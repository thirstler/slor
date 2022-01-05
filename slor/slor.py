#!/usr/bin/env python3

import sys
from shared import *

if __name__ == "__main__":

    if len(sys.argv) < 2:
        sys.stderr.write(ROOT_HELP)

    elif sys.argv[1] == "controller":
        import control

        control.run()

    elif sys.argv[1] == "driver":
        import driver as driver

        driver.run()

    else:
        sys.stderr.write(ROOT_HELP)
