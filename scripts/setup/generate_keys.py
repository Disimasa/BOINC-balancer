#!/usr/bin/env python3
import sys
from lib.keys import generate_signing_keys

if __name__ == "__main__":
    if not generate_signing_keys():
        sys.exit(1)
    
    print("\n" + "=" * 80)
    print("✓ ГЕНЕРАЦИЯ КЛЮЧЕЙ ЗАВЕРШЕНА УСПЕШНО")
    print("=" * 80)
