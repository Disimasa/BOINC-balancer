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
import json
from pathlib import Path

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


def step_update_cache_config():
    """Шаг 2.5: Обновление конфигурации кеширования после создания проекта"""
    print("\n" + "=" * 80)
    print("ШАГ 2.5: Обновление конфигурации кеширования")
    print("=" * 80)
    
    # Получаем имя проекта из переменных окружения
    project_name = os.environ.get("PROJECT", "boincserver")
    project_root = os.environ.get("PROJECT_ROOT", "/home/boincadm/project")
    
    # Определяем имя контейнера apache
    # Получаем имя контейнера через docker compose ps
    cmd = ["wsl.exe", "-e", "docker", "compose", "ps", "--format", "json", "apache"]
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=SCRIPT_DIR)
    if result.returncode != 0 or not result.stdout.strip():
        # Пробуем альтернативный способ - получить имя через docker ps
        cmd = ["wsl.exe", "-e", "docker", "ps", "--filter", "name=apache", "--format", "{{.Names}}"]
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=SCRIPT_DIR)
        if result.returncode != 0 or not result.stdout.strip():
            print("⚠ Предупреждение: контейнер apache не найден, пропускаем обновление кеша")
            return True
        container_name = result.stdout.strip().split('\n')[0]
    else:
        # Парсим JSON (упрощенный вариант - берем первое имя)
        try:
            lines = [l for l in result.stdout.strip().split('\n') if l]
            if lines:
                container_data = json.loads(lines[0])
                container_name = container_data.get('Name', '')
            else:
                print("⚠ Предупреждение: контейнер apache не найден, пропускаем обновление кеша")
                return True
        except:
            # Fallback: используем стандартное имя
            container_name = "server-apache-1"
    
    if not container_name:
        print("⚠ Предупреждение: не удалось определить имя контейнера apache, пропускаем обновление кеша")
        return True
    
    # Ждем, пока проект будет создан (проверяем наличие .built_${PROJECT})
    print(f"Ожидание создания проекта (проверка файла .built_{project_name})...")
    max_wait = 120  # максимум 2 минуты
    waited = 0
    while waited < max_wait:
        cmd = ["wsl.exe", "-e", "docker", "exec", container_name, "bash", "-c", 
               f"test -f {project_root}/.built_{project_name} && echo 'ready' || echo 'waiting'"]
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=SCRIPT_DIR)
        if "ready" in result.stdout:
            print("✓ Проект создан")
            break
        time.sleep(2)
        waited += 2
        if waited % 10 == 0:
            print(f"  Ожидание... ({waited}/{max_wait} сек)")
    else:
        print("⚠ Предупреждение: проект не создан за отведенное время, продолжаем...")
    
    # Копируем файл cache_parameters.inc в контейнер
    cache_file_path = SCRIPT_DIR / "images" / "apache" / "cache_parameters.inc"
    if not cache_file_path.exists():
        print(f"✗ Ошибка: файл {cache_file_path} не найден", file=sys.stderr)
        return False
    
    print(f"Копирование {cache_file_path} в контейнер...")
    cmd = ["wsl.exe", "-e", "docker", "cp", str(cache_file_path), 
           f"{container_name}:{project_root}/html/project/cache_parameters.inc"]
    if not run_command(cmd, check=True, cwd=SCRIPT_DIR):
        return False
    
    # Устанавливаем правильные права на файл
    cmd = ["wsl.exe", "-e", "docker", "exec", container_name, "bash", "-c",
           f"chown boincadm:boincadm {project_root}/html/project/cache_parameters.inc"]
    if not run_command(cmd, check=True, cwd=SCRIPT_DIR):
        return False
    
    # Проверяем содержимое файла
    cmd = ["wsl.exe", "-e", "docker", "exec", container_name, "bash", "-c",
           f"grep -q 'STATUS_PAGE_TTL.*0' {project_root}/html/project/cache_parameters.inc && echo 'OK' || echo 'FAIL'"]
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=SCRIPT_DIR)
    if "OK" not in result.stdout:
        print("⚠ Предупреждение: файл не содержит ожидаемых значений TTL=0", file=sys.stderr)
    else:
        print("✓ Конфигурация кеширования обновлена (кеширование отключено)")
    
    return True


def step_create_apps():
    """Шаг 3: Создание приложений"""
    print("\n" + "=" * 80)
    print("ШАГ 3: Создание приложений")
    print("=" * 80)
    
    return run_python_script("create_apps.py")


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
    
    return run_python_script("create_tasks_bin.py")


def main():
    """Главная функция pipeline"""
    print("\n" + "=" * 80)
    print("BOINC PROJECT SETUP PIPELINE")
    print("=" * 80)
    
    steps = [
        ("Очистка", step_cleanup),
        ("Сборка и запуск", step_build),
        ("Обновление конфигурации кеширования", step_update_cache_config),
        ("Создание приложений", step_create_apps),
        ("Создание задач", step_create_tasks),
        ("Создание пользователя", step_create_user),
        ("Подключение клиентов", step_connect_clients),
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
    print("  ✓ Приложения созданы")
    print("  ✓ Пользователь создан")
    print("  ✓ Клиенты подключены")
    print("  ✓ Задачи созданы")
    print("\nПроект готов к работе!")


if __name__ == "__main__":
    main()

