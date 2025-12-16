#!/usr/bin/env python3
import sys
import argparse
from lib.clients import connect_all_clients

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=20)
    args = parser.parse_args()
    
    success = connect_all_clients(count=args.count)
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())
