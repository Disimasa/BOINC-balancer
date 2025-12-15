#!/usr/bin/env python3
import sys
import argparse
from lib.clients import connect_all_clients

def main():
    parser = argparse.ArgumentParser(description="Подключение клиентов BOINC к серверу")
    parser.add_argument("--count", type=int, default=20, help="Количество клиентов для подключения")
    args = parser.parse_args()
    
    success = connect_all_clients(count=args.count)
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())
