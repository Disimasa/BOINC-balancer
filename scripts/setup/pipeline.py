#!/usr/bin/env python3
import sys
from pathlib import Path
from lib.pipeline import run_full_pipeline


def run_pipeline():
    success, account_key = run_full_pipeline(balance_hosts=False, client_count=20, update_clients=True)
    if not success:
        sys.exit(1)
    return success, account_key


if __name__ == "__main__":
    run_pipeline()
