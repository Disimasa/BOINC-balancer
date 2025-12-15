#!/usr/bin/env python3
import sys
from lib.users import create_or_lookup_user

if __name__ == "__main__":
    success, account_key = create_or_lookup_user()
    sys.exit(0 if success else 1)
