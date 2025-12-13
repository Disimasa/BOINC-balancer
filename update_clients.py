#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Принудительное обновление всех клиентов для запроса задач
"""
import subprocess
import sys
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent.absolute()

def run_command(cmd, check=True):
    """Выполнить команду"""
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=60
        )
        if check and result.returncode != 0:
            print(f"Ошибка: {result.stderr}", file=sys.stderr)
            return False
        return True
    except Exception as e:
        print(f"Исключение: {e}", file=sys.stderr)
        return False

def update_all_clients():
    """Принудительно обновить все клиенты"""
    print("\n" + "=" * 80)
    print("ПРИНУДИТЕЛЬНОЕ ОБНОВЛЕНИЕ КЛИЕНТОВ")
    print("=" * 80)
    
    # Получаем список всех клиентов
    cmd = "docker ps --format '{{.Names}}' | grep '^boinc-client-'"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode != 0:
        print("Не удалось получить список клиентов", file=sys.stderr)
        return False
    
    clients = [line.strip() for line in result.stdout.strip().split('\n') if line.strip()]
    
    if not clients:
        print("Клиенты не найдены")
        return True
    
    print(f"\nНайдено клиентов: {len(clients)}")
    print("Обновляю клиентов...")
    
    project_url = "http://172.26.176.1/boincserver/"
    updated = 0
    failed = 0
    
    for client_name in clients:
        cmd = f"docker exec {client_name} boinccmd --project {project_url} update 2>&1"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            updated += 1
            print(f"  ✓ {client_name}")
        else:
            failed += 1
            print(f"  ✗ {client_name}: {result.stderr[:50]}")
    
    print(f"\n✓ Обновлено: {updated}, ошибок: {failed}")
    
    if updated > 0:
        print("\nОжидание 5 секунд для обработки запросов...")
        time.sleep(5)
    
    return updated > 0

if __name__ == "__main__":
    success = update_all_clients()
    sys.exit(0 if success else 1)



