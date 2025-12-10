#!/usr/bin/env python3
"""Скрипт для проверки статуса задач в BOINC"""
import subprocess
import sys

CONTAINER_NAME = "server-apache-1"
PROJECT_HOME = "/home/boincadm/project"

def run_command(cmd):
    """Выполнить команду в контейнере apache"""
    full_cmd = [
        "wsl.exe", "-e", "docker", "exec", CONTAINER_NAME,
        "bash", "-c", cmd
    ]
    try:
        result = subprocess.run(full_cmd, capture_output=True, text=True, timeout=10)
        return result.stdout, result.stderr, result.returncode
    except Exception as e:
        return None, str(e), 1

print("=== Проверка статуса задач ===\n")

# Проверяем приложения
print("Приложения:")
stdout, stderr, code = run_command(f"cd {PROJECT_HOME} && bin/query_db 'SELECT name, id FROM app'")
if code == 0:
    print(stdout)
else:
    print(f"Ошибка: {stderr}")

# Проверяем количество задач
print("\nКоличество задач:")
stdout, stderr, code = run_command(f"cd {PROJECT_HOME} && bin/query_db 'SELECT COUNT(*) as total FROM workunit'")
if code == 0:
    print(stdout)
else:
    print(f"Ошибка: {stderr}")

# Проверяем статус задач
print("\nСтатус задач (первые 10):")
stdout, stderr, code = run_command(f"cd {PROJECT_HOME} && bin/query_db 'SELECT name, appid, target_nresults, priority, transition_time FROM workunit LIMIT 10'")
if code == 0:
    print(stdout)
else:
    print(f"Ошибка: {stderr}")

# Проверяем, обновлены ли версии
print("\nПроверка версий приложений:")
stdout, stderr, code = run_command(f"cd {PROJECT_HOME} && bin/update_versions 2>&1")
if code == 0:
    print("Версии обновлены")
    if stdout:
        print(stdout)
else:
    print(f"Ошибка при обновлении версий: {stderr}")

