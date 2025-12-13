#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pipeline для полного setup BOINC проекта:
1. Остановка и очистка контейнеров и volumes
2. Сборка и запуск контейнеров
3. Создание приложений
4. Создание пользователя
5. Подключение клиентов
6. Создание задач
"""

from __future__ import print_function
import subprocess
import sys
import time
import os
from pathlib import Path

# Импортируем функцию генерации ключей
from generate_keys import generate_signing_keys

# Определяем директорию скрипта
SCRIPT_DIR = Path(__file__).parent.absolute()

def run_command(cmd, check=True, cwd=None, shell=False):
    """Выполнить команду с выводом логов в реальном времени"""
    print("\n" + "=" * 80)
    print(f"Выполняю: {' '.join(cmd) if isinstance(cmd, list) else cmd}")
    print("=" * 80)
    
    try:
        if isinstance(cmd, str):
            proc = subprocess.Popen(
                cmd,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1,
                cwd=cwd
            )
        else:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1,
                cwd=cwd
            )
        
        # Выводим логи в реальном времени
        for line in proc.stdout:
            print(line, end='', flush=True)
        
        proc.wait()
        
        if check and proc.returncode != 0:
            print(f"\n✗ Ошибка: команда завершилась с кодом {proc.returncode}", file=sys.stderr)
            return False
        
        return True
    except Exception as e:
        print(f"\n✗ Исключение при выполнении команды: {e}", file=sys.stderr)
        return False


def run_python_script(script_name, *args):
    """Запустить Python скрипт с выводом логов"""
    script_path = SCRIPT_DIR / script_name
    if not script_path.exists():
        print(f"✗ Ошибка: скрипт {script_name} не найден", file=sys.stderr)
        return False
    
    cmd = [sys.executable, str(script_path)] + list(args)
    return run_command(cmd, check=True, cwd=SCRIPT_DIR)


def step_cleanup():
    """Шаг 1: Остановка и очистка контейнеров и volumes"""
    print("\n" + "=" * 80)
    print("ШАГ 1: Остановка и очистка контейнеров и volumes")
    print("=" * 80)
    
    # Останавливаем контейнеры
    cmd = ["wsl.exe", "-e", "docker", "compose", "down", "-v"]
    if not run_command(cmd, check=False, cwd=SCRIPT_DIR):
        print("⚠ Предупреждение: ошибка при остановке контейнеров, продолжаем...")
    
    # Дополнительная очистка volumes (на случай если они не удалились)
    cmd = ["wsl.exe", "-e", "docker", "volume", "prune", "-f"]
    run_command(cmd, check=False, cwd=SCRIPT_DIR)
    
    print("✓ Очистка завершена")
    return True


def step_build():
    """Шаг 2: Сборка и запуск контейнеров"""
    print("\n" + "=" * 80)
    print("ШАГ 2: Сборка и запуск контейнеров")
    print("=" * 80)
    
    cmd = ["wsl.exe", "-e", "docker", "compose", "up", "-d", "--build"]
    if not run_command(cmd, check=True, cwd=SCRIPT_DIR):
        return False
    
    print("\nОжидание запуска контейнеров...")
    time.sleep(10)
    
    # Проверяем статус контейнеров
    cmd = ["wsl.exe", "-e", "docker", "compose", "ps"]
    run_command(cmd, check=False, cwd=SCRIPT_DIR)
    
    print("✓ Контейнеры запущены")
    return True


def step_generate_keys():
    """Шаг 2.5: Генерация ключей подписи кода"""
    return generate_signing_keys()


def step_update_cache_config():
    """Шаг 2.6: Обновление конфигурации кеширования после создания проекта"""
    print("\n" + "=" * 80)
    print("ШАГ 2.6: Обновление конфигурации кеширования")
    print("=" * 80)
    
    project_name = os.environ.get("PROJECT", "boincserver")
    project_root = os.environ.get("PROJECT_ROOT", "/home/boincadm/project")
    container_name = "server-apache-1"
    
    # Ждем создания проекта
    print(f"Ожидание создания проекта...")
    for i in range(60):
        cmd = ["wsl.exe", "-e", "docker", "exec", container_name, "bash", "-c", 
               f"test -f {project_root}/.built_{project_name} && echo ready"]
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=SCRIPT_DIR)
        if "ready" in result.stdout:
            break
        time.sleep(2)
    
    # Копируем cache_parameters.inc
    cache_file = SCRIPT_DIR / "images" / "apache" / "cache_parameters.inc"
    if not cache_file.exists():
        print(f"⚠ Файл {cache_file} не найден", file=sys.stderr)
        return True
    
    run_command(["wsl.exe", "-e", "docker", "cp", str(cache_file), 
                 f"{container_name}:{project_root}/html/project/cache_parameters.inc"], check=False)
    run_command(["wsl.exe", "-e", "docker", "exec", container_name, "bash", "-c",
                 f"chown boincadm:boincadm {project_root}/html/project/cache_parameters.inc"], check=False)
    
    print("✓ Конфигурация кеширования обновлена")
    return True


def step_create_apps():
    """Шаг 3: Создание приложений"""
    print("\n" + "=" * 80)
    print("ШАГ 3: Создание приложений")
    print("=" * 80)
    
    return run_python_script("create_apps.py")


def step_start_daemons():
    """Шаг 3.5: Запуск валидаторов и ассимиляторов"""
    print("\n" + "=" * 80)
    print("ШАГ 3.5: Запуск валидаторов и ассимиляторов")
    print("=" * 80)
    
    # Даем время контейнеру полностью запуститься
    print("Ожидание готовности контейнера...")
    time.sleep(3)
    
    # Импортируем функцию запуска демонов
    try:
        from start_daemons import start_all_daemons
        result = start_all_daemons()
        if result:
            print("\n✓ Валидаторы и ассимиляторы успешно запущены")
        else:
            print("\n⚠ Предупреждение: некоторые валидаторы или ассимиляторы не запустились", file=sys.stderr)
        return result
    except Exception as e:
        print(f"\n✗ Ошибка при запуске валидаторов и ассимиляторов: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return False


def step_create_user():
    """Шаг 4: Создание пользователя"""
    print("\n" + "=" * 80)
    print("ШАГ 4: Создание пользователя")
    print("=" * 80)
    
    return run_python_script("create_user.py")


def step_connect_clients():
    """Шаг 5: Подключение клиентов"""
    print("\n" + "=" * 80)
    print("ШАГ 5: Подключение клиентов")
    print("=" * 80)
    
    # Даем время серверу полностью запуститься
    print("Ожидание готовности сервера...")
    time.sleep(5)
    
    return run_python_script("connect_clients.py", "--count", "20")


def step_create_tasks():
    """Шаг 6: Создание задач (нативные бинарные задачи)"""
    print("\n" + "=" * 80)
    print("ШАГ 6: Создание задач")
    print("=" * 80)
    
    # Даем время клиентам подключиться
    print("Ожидание подключения клиентов...")
    time.sleep(5)
    
    # Создаем задачи
    if not run_python_script("create_tasks_bin.py"):
        return False
    
    # Принудительно обновляем клиентов, чтобы они запросили задачи
    print("\n" + "=" * 80)
    print("ШАГ 6.5: Принудительное обновление клиентов")
    print("=" * 80)
    return run_python_script("update_clients.py")


def main():
    """Главная функция pipeline"""
    print("\n" + "=" * 80)
    print("BOINC PROJECT SETUP PIPELINE")
    print("=" * 80)
    
    steps = [
        ("Очистка", step_cleanup),
        ("Сборка и запуск", step_build),
        ("Генерация ключей подписи", step_generate_keys),
        ("Обновление конфигурации кеширования", step_update_cache_config),
        ("Создание приложений", step_create_apps),  # Приложения создаются ДО подключения клиентов
        ("Запуск валидаторов и ассимиляторов", step_start_daemons),  # Валидаторы и ассимиляторы запускаются после создания приложений
        ("Создание пользователя", step_create_user),
        ("Подключение клиентов", step_connect_clients),  # Клиенты подключаются ПОСЛЕ создания приложений
        ("Создание задач", step_create_tasks),
    ]
    
    for step_name, step_func in steps:
        try:
            if not step_func():
                print(f"\n✗ Ошибка на шаге '{step_name}'. Прерывание pipeline.", file=sys.stderr)
                sys.exit(1)
        except KeyboardInterrupt:
            print("\n\n⚠ Pipeline прерван пользователем", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"\n✗ Неожиданная ошибка на шаге '{step_name}': {e}", file=sys.stderr)
            sys.exit(1)
    
    print("\n" + "=" * 80)
    print("✓ PIPELINE УСПЕШНО ЗАВЕРШЕН!")
    print("=" * 80)
    print("\nВсе шаги выполнены успешно:")
    print("  ✓ Контейнеры запущены")
    print("  ✓ Ключи подписи сгенерированы")
    print("  ✓ Приложения созданы")
    print("  ✓ Валидаторы и ассимиляторы запущены")
    print("  ✓ Пользователь создан")
    print("  ✓ Клиенты подключены")
    print("  ✓ Задачи созданы")
    print("\nПроект готов к работе!")


if __name__ == "__main__":
    main()

