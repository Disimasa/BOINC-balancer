#!/usr/bin/env python3
import sys
from lib.clients import update_all_clients

if __name__ == "__main__":
    success = update_all_clients()
    sys.exit(0 if success else 1)
