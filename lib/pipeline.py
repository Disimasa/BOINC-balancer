#!/usr/bin/env python3
import sys
import time
import os
from pathlib import Path
from lib.utils import run_local_command, SCRIPT_DIR, SERVER_DIR
from lib.keys import generate_signing_keys
from lib.apps import create_all_apps
from lib.daemons import start_all_daemons
from lib.users import create_or_lookup_user
from lib.clients import connect_all_clients, update_all_clients
from scripts.management.create_tasks_bin import create_tasks

def cleanup():
    print("\n" + "=" * 80)
    print("ШАГ 1: Остановка и очистка контейнеров и volumes")
    print("=" * 80)
    
    result = run_local_command(["docker", "compose", "down", "-v"], check=False, cwd=SCRIPT_DIR)
    if result.returncode != 0:
        print("⚠ Предупреждение: ошибка при остановке контейнеров, продолжаем...", file=sys.stderr)
    
    run_local_command(["docker", "volume", "prune", "-f"], check=False, cwd=SCRIPT_DIR)
    
    return True


def build():
    print("\n" + "=" * 80)
    print("ШАГ 2: Сборка и запуск контейнеров")
    print("=" * 80)
    
    result = run_local_command(["docker", "compose", "up", "-d", "--build"], check=True, cwd=SERVER_DIR)
    if result.returncode != 0:
        return False
    
    time.sleep(10)
    
    return True


def update_cache_config():
    print("\n" + "=" * 80)
    print("ШАГ 2.6: Обновление конфигурации кеширования")
    print("=" * 80)
    
    project_name = os.environ.get("PROJECT", "boincserver")
    project_root = os.environ.get("PROJECT_ROOT", "/home/boincadm/project")
    container_name = "server-apache-1"
    
    for i in range(60):
        result = run_local_command(["docker", "exec", container_name, "bash", "-c", 
               f"test -f {project_root}/.built_{project_name} && echo ready"],
               capture_output=True, cwd=SERVER_DIR)
        if result.returncode == 0 and "ready" in result.stdout:
            break
        time.sleep(2)
    
    cache_file = SERVER_DIR / "docker" / "apache" / "cache_parameters.inc"
    if not cache_file.exists():
        print(f"⚠ Файл {cache_file} не найден", file=sys.stderr)
        return True
    
    run_local_command(["docker", "cp", str(cache_file), 
                 f"{container_name}:{project_root}/html/project/cache_parameters.inc"], 
                 check=False, capture_output=True)
    run_local_command(["docker", "exec", container_name, "bash", "-c",
                 f"chown boincadm:boincadm {project_root}/html/project/cache_parameters.inc"], check=False)
    
    return True


def run_full_pipeline(balance_hosts=False, client_count=20, update_clients=True):
    print("\n" + "=" * 80)
    print("BOINC PROJECT SETUP PIPELINE")
    print("=" * 80)
    
    account_key = None
    
    def step_start_daemons():
        time.sleep(3)
        return start_all_daemons()
    
    def step_connect_clients():
        nonlocal account_key
        success, key = create_or_lookup_user()
        if not success:
            return False
        account_key = key
        time.sleep(5)
        return connect_all_clients(count=client_count, account_key=account_key)
    
    def step_create_tasks():
        time.sleep(5)
        return create_tasks(balance_hosts=balance_hosts)
    
    def step_update_clients():
        if update_clients:
            return update_all_clients()
        return True
    
    steps = [
        ("Очистка", cleanup),
        ("Сборка и запуск", build),
        ("Генерация ключей подписи", generate_signing_keys),
        ("Обновление конфигурации кеширования", update_cache_config),
        ("Создание приложений", create_all_apps),
        ("Запуск валидаторов и ассимиляторов", step_start_daemons),
        ("Создание пользователя и подключение клиентов", step_connect_clients),
        ("Создание задач", step_create_tasks),
        ("Обновление клиентов", step_update_clients),
    ]
    
    for step_name, step_func in steps:
        try:
            result = step_func()
            if result is False:
                print(f"\n✗ Ошибка на шаге '{step_name}'", file=sys.stderr)
                return False, None
        except KeyboardInterrupt:
            print("\n\n⚠ Pipeline прерван пользователем", file=sys.stderr)
            return False, None
        except Exception as e:
            print(f"\n✗ Неожиданная ошибка на шаге '{step_name}': {e}", file=sys.stderr)
            return False, None
    
    return True, account_key

