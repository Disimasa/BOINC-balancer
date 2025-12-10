#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Проверка состояния результатов и версий приложений"""
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

# Проверяем результаты
print("=" * 60)
print("Проверка результатов")
print("=" * 60)
sql = "SELECT server_state, COUNT(*) as cnt FROM result GROUP BY server_state"
stdout, stderr, code = run_command(
    "mysql -u boincadm -pboincadm boincserver -e '{}'".format(sql)
)
print(stdout)
if stderr:
    print("STDERR:", stderr)

# Проверяем версии приложений
print("\n" + "=" * 60)
print("Проверка версий приложений")
print("=" * 60)
sql = "SELECT id, app_name, version_num, platform FROM app_version WHERE app_name IN ('fast_task', 'medium_task') ORDER BY id DESC LIMIT 5"
stdout, stderr, code = run_command(
    "mysql -u boincadm -pboincadm boincserver -e '{}'".format(sql)
)
print(stdout)
if stderr:
    print("STDERR:", stderr)

# Проверяем связь результатов с версиями
print("\n" + "=" * 60)
print("Связь результатов с версиями приложений")
print("=" * 60)
sql = "SELECT r.app_version_id, av.app_name, COUNT(*) as cnt FROM result r LEFT JOIN app_version av ON r.app_version_id = av.id WHERE r.server_state = 2 GROUP BY r.app_version_id LIMIT 5"
stdout, stderr, code = run_command(
    "mysql -u boincadm -pboincadm boincserver -e '{}'".format(sql)
)
print(stdout)
if stderr:
    print("STDERR:", stderr)

# Проверяем workunits
print("\n" + "=" * 60)
print("Проверка workunits")
print("=" * 60)
sql = "SELECT name, appid, transition_time FROM workunit WHERE name LIKE '%native%' ORDER BY id DESC LIMIT 5"
stdout, stderr, code = run_command(
    "mysql -u boincadm -pboincadm boincserver -e '{}'".format(sql)
)
print(stdout)
if stderr:
    print("STDERR:", stderr)

