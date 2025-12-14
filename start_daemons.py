#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Запуск валидаторов и ассимиляторов для BOINC проекта
"""
from __future__ import print_function
import subprocess
import sys
import time
import os

PROJECT_HOME = "/home/boincadm/project"
CONTAINER_NAME = "server-apache-1"

# Список приложений для которых нужно запустить валидаторы и ассимиляторы
APPS = ["fast_task", "medium_task", "long_task", "random_task"]


def run_command(cmd, check=True, capture_output=False):
    """Выполнить команду в контейнере apache"""
    full_cmd = ["wsl.exe", "-e", "docker", "exec", CONTAINER_NAME, "bash", "-c", 
                f"export BOINC_PROJECT_DIR={PROJECT_HOME} && cd {PROJECT_HOME} && {cmd}"]
    try:
        result = subprocess.run(full_cmd, capture_output=True, text=True, check=check)
        if capture_output:
            return result.stdout.strip()
        else:
            print(result.stdout, end='')
            if result.stderr:
                print(result.stderr, file=sys.stderr)
            return result.returncode == 0
    except subprocess.CalledProcessError as e:
        print(f"✗ Ошибка выполнения команды: {e}", file=sys.stderr)
        if capture_output:
            return ""
        return False
    except FileNotFoundError:
        print("✗ Ошибка: WSL или Docker не найдены", file=sys.stderr)
        return False


def check_validator_running(app_name):
    """Проверить, запущен ли валидатор для приложения"""
    cmd = f"ps aux | grep '[s]ample_trivial_validator -app {app_name}'"
    output = run_command(cmd, check=False, capture_output=True)
    return bool(output and output.strip())


def check_assimilator_running(app_name):
    """Проверить, запущен ли ассимилятор для приложения"""
    cmd = f"ps aux | grep '[s]cript_assimilator.*--app {app_name}'"
    output = run_command(cmd, check=False, capture_output=True)
    return bool(output and output.strip())


def start_validator(app_name):
    """Запустить валидатор для приложения"""
    if check_validator_running(app_name):
        print(f"  ✓ Валидатор для {app_name} уже запущен")
        return True
    
    print(f"  Запускаю валидатор для {app_name}...")
    cmd = f"mkdir -p logs && nohup bin/sample_trivial_validator -app {app_name} > logs/validator_{app_name}.log 2>&1 &"
    if run_command(cmd, check=False):
        time.sleep(1)  # Даем время процессу запуститься
        if check_validator_running(app_name):
            print(f"  ✓ Валидатор для {app_name} запущен")
            return True
        else:
            print(f"  ✗ Валидатор для {app_name} не запустился", file=sys.stderr)
            return False
    else:
        print(f"  ✗ Ошибка запуска валидатора для {app_name}", file=sys.stderr)
        return False


def start_assimilator(app_name):
    """Запустить ассимилятор для приложения"""
    if check_assimilator_running(app_name):
        print(f"  ✓ Ассимилятор для {app_name} уже запущен")
        return True
    
    print(f"  Запускаю ассимилятор для {app_name}...")
    # Убеждаемся, что символическая ссылка существует (script_assimilator ищет скрипты в ../bin/)
    run_command(f"mkdir -p ../bin && ln -sf {PROJECT_HOME}/bin/{app_name}_assimilator ../bin/{app_name}_assimilator", check=False)
    
    # Проверяем, что скрипт ассимилятора существует
    check_script_cmd = f"test -f bin/{app_name}_assimilator && echo 'exists' || echo 'missing'"
    script_check = run_command(check_script_cmd, check=False, capture_output=True)
    if "missing" in script_check:
        print(f"  ⚠ Предупреждение: скрипт bin/{app_name}_assimilator не найден", file=sys.stderr)
    
    cmd = f"mkdir -p logs && PATH={PROJECT_HOME}/bin:$PATH nohup bin/script_assimilator --app {app_name} --script \"{app_name}_assimilator files\" > logs/assimilator_{app_name}.log 2>&1 &"
    if run_command(cmd, check=False):
        time.sleep(1)  # Даем время процессу запуститься
        if check_assimilator_running(app_name):
            print(f"  ✓ Ассимилятор для {app_name} запущен")
            return True
        else:
            print(f"  ✗ Ассимилятор для {app_name} не запустился", file=sys.stderr)
            # Показываем последние строки лога для отладки
            log_check = run_command(f"tail -5 logs/assimilator_{app_name}.log 2>/dev/null || echo 'Log not found'", check=False, capture_output=True)
            if log_check:
                print(f"    Лог: {log_check}", file=sys.stderr)
            return False
    else:
        print(f"  ✗ Ошибка запуска ассимилятора для {app_name}", file=sys.stderr)
        return False


def start_all_daemons():
    """Запустить все валидаторы и ассимиляторы"""
    print("\n" + "=" * 80)
    print("ЗАПУСК ВАЛИДАТОРОВ И АССИМИЛЯТОРОВ")
    print("=" * 80)
    
    success = True
    
    # Запускаем валидаторы
    print("\nЗапуск валидаторов:")
    for app_name in APPS:
        if not start_validator(app_name):
            success = False
    
    # Запускаем ассимиляторы
    print("\nЗапуск ассимиляторов:")
    for app_name in APPS:
        if not start_assimilator(app_name):
            success = False
    
    # Проверяем статус
    print("\n" + "=" * 80)
    print("ПРОВЕРКА СТАТУСА")
    print("=" * 80)
    
    print("\nЗапущенные валидаторы:")
    cmd = "ps aux | grep '[s]ample_trivial_validator' | grep -v grep"
    output = run_command(cmd, check=False, capture_output=True)
    if output:
        for line in output.split('\n'):
            if line.strip():
                print(f"  {line}")
    else:
        print("  Нет запущенных валидаторов")
    
    print("\nЗапущенные ассимиляторы:")
    cmd = "ps aux | grep '[s]cript_assimilator' | grep -v grep"
    output = run_command(cmd, check=False, capture_output=True)
    if output:
        for line in output.split('\n'):
            if line.strip():
                print(f"  {line}")
    else:
        print("  Нет запущенных ассимиляторов")
    
    if success:
        print("\n✓ Все валидаторы и ассимиляторы запущены")
    else:
        print("\n⚠ Некоторые валидаторы или ассимиляторы не запустились", file=sys.stderr)
    
    return success


if __name__ == "__main__":
    if not start_all_daemons():
        sys.exit(1)

