#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Проверка статуса задач и результатов"""
import subprocess
import sys

CONTAINER_NAME = "server-apache-1"
PROJECT_HOME = "/home/boincadm/project"

def run_command(cmd):
    """Выполнить команду в контейнере"""
    full_cmd = [
        "wsl.exe", "-e", "docker", "exec", CONTAINER_NAME,
        "bash", "-lc", "export BOINC_PROJECT_DIR={proj} && cd {proj} && {cmd}".format(
            proj=PROJECT_HOME, cmd=cmd
        )
    ]
    try:
        proc = subprocess.Popen(
            full_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        stdout, stderr = proc.communicate()
        return stdout, stderr, proc.returncode
    except Exception as e:
        print("Ошибка: {}".format(e), file=sys.stderr)
        return "", str(e), 1

print("=" * 60)
print("Проверка статуса задач")
print("=" * 60)

# Проверяем workunits
print("\n1. Workunits:")
stdout, stderr, code = run_command(
    "bin/db_query \"SELECT COUNT(*) as total FROM workunit WHERE name LIKE '%native%'\""
)
print(stdout if stdout else "Ошибка: {}".format(stderr))

# Проверяем результаты
print("\n2. Результаты по состояниям:")
stdout, stderr, code = run_command(
    "bin/db_query \"SELECT server_state, COUNT(*) as cnt FROM result GROUP BY server_state\""
)
print(stdout if stdout else "Ошибка: {}".format(stderr))

# Проверяем результаты для fast_task
print("\n3. Результаты для fast_task:")
stdout, stderr, code = run_command(
    "bin/db_query \"SELECT COUNT(*) as total FROM result r JOIN workunit w ON r.workunitid=w.id WHERE w.name LIKE 'fast_task%'\""
)
print(stdout if stdout else "Ошибка: {}".format(stderr))

# Проверяем версии приложений
print("\n4. Версии приложений:")
stdout, stderr, code = run_command(
    "bin/db_query \"SELECT id, app_name, version_num, platform FROM app_version WHERE app_name='fast_task' ORDER BY id DESC LIMIT 3\""
)
print(stdout if stdout else "Ошибка: {}".format(stderr))


