#!/usr/bin/env python3
import sys
from lib.daemons import start_all_daemons

if __name__ == "__main__":
    if not start_all_daemons():
        sys.exit(1)
