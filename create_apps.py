#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для создания Apps в BOINC и настройки валидаторов/ассимиляторов
"""

from __future__ import print_function
import subprocess
import sys
import time

PROJECT_HOME = "/home/boincadm/project"
CONTAINER_NAME = "server-apache-1"

apps = [
    {"name": "fast_task", "resultsdir": "/results/fast_task"},
    {"name": "medium_task", "resultsdir": "/results/medium_task"},
    {"name": "long_task", "resultsdir": "/results/long_task"},
    {"name": "random_task", "resultsdir": "/results/random_task"}
]


def run_cmd(cmd, check=True):
    """Выполнить команду в контейнере"""
    full_cmd = ["wsl.exe", "-e", "docker", "exec", CONTAINER_NAME, "bash", "-c", cmd]
    proc = subprocess.Popen(full_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    stdout, stderr = proc.communicate()
    if stdout:
        print(stdout, end='')
    if stderr:
        print(stderr, file=sys.stderr, end='')
    if check and proc.returncode != 0:
        print("Ошибка: команда завершилась с кодом {}".format(proc.returncode), file=sys.stderr)
        sys.exit(1)
    return proc.returncode == 0


def create_app(app_name, resultsdir):
    """Создать приложение"""
    print("\nСоздаю App: {}".format(app_name))
    
    # Структура директорий
    run_cmd("cd {} && mkdir -p apps/{}/1.0/x86_64-pc-linux-gnu".format(PROJECT_HOME, app_name), check=False)
    
    # Assimilator скрипт
    run_cmd("""cd {} && cat > bin/{}_assimilator << 'EOF'
#!/bin/bash
RESULTS_DIR={}
mkdir -p "$RESULTS_DIR"
for file in "$@"; do
    [ -f "$file" ] && cp "$file" "$RESULTS_DIR/"
done
EOF
chmod +x bin/{}_assimilator && chown boincadm:boincadm bin/{}_assimilator""".format(
        PROJECT_HOME, app_name, resultsdir, app_name, app_name
    ), check=False)
    
    # Добавляем в config.xml
    run_cmd("""cd {} && if ! grep -q 'sample_trivial_validator.*{}' config.xml; then
        sed -i '/<\\/daemons>/i\\        <daemon>\\n            <cmd>sample_trivial_validator -app {}</cmd>\\n        </daemon>' config.xml
    fi""".format(PROJECT_HOME, app_name, app_name), check=False)
    
    run_cmd("""cd {} && if ! grep -q '{}_assimilator' config.xml; then
        sed -i '/<\\/daemons>/i\\        <daemon>\\n            <cmd>script_assimilator --app {} --script "{}_assimilator files"</cmd>\\n        </daemon>' config.xml
    fi""".format(PROJECT_HOME, app_name, app_name, app_name), check=False)
    
    # Добавляем в project.xml
    run_cmd("""cd {} && if ! grep -q '<name>{}</name>' project.xml; then
        sed -i '/<\\/boinc>/i\\    <app>\\n        <name>{}</name>\\n        <user_friendly_name>{}</user_friendly_name>\\n    </app>\\n' project.xml
    fi""".format(PROJECT_HOME, app_name, app_name, app_name), check=False)
    
    # Добавляем в БД
    run_cmd("cd {} && bin/xadd".format(PROJECT_HOME), check=False)
    
    return True


def setup_daemons():
    """Настроить и запустить валидаторы и ассимиляторы"""
    # Символические ссылки для ассимиляторов (script_assimilator использует ../bin/)
    apps_list = " ".join([app['name'] for app in apps])
    run_cmd("""cd {} && mkdir -p ../bin && for app in {}; do
        ln -sf {}/bin/${{app}}_assimilator ../bin/${{app}}_assimilator
    done""".format(PROJECT_HOME, apps_list, PROJECT_HOME), check=False)
    
    # Права доступа на директории результатов
    results_dirs = " ".join([app['resultsdir'] for app in apps])
    run_cmd("mkdir -p {} && chown -R boincadm:boincadm /results && chmod -R 755 /results".format(results_dirs), check=False)
    
    # Перезапуск демонов
    run_cmd("cd {} && bin/stop && sleep 2 && bin/start".format(PROJECT_HOME), check=False)
    
    # Запуск валидаторов и ассимиляторов
    for app in apps:
        app_name = app['name']
        run_cmd("""cd {} && su boincadm -c 'cd {} && PATH={}/bin:$PATH nohup {}/bin/sample_trivial_validator -app {} > /dev/null 2>&1 &'""".format(
            PROJECT_HOME, PROJECT_HOME, PROJECT_HOME, PROJECT_HOME, app_name
        ), check=False)
        run_cmd("""cd {} && su boincadm -c 'cd {} && PATH={}/bin:$PATH nohup bin/script_assimilator --app {} --script "{}_assimilator files" > /dev/null 2>&1 &'""".format(
            PROJECT_HOME, PROJECT_HOME, PROJECT_HOME, app_name, app_name
        ), check=False)
        print("  ✓ {}: валидатор и ассимилятор запущены".format(app_name))
    
    time.sleep(2)


def create_apps():
    """Создать все Apps"""
    print("=" * 60)
    print("Создание Apps...")
    print("=" * 60)
    
    for app in apps:
        create_app(app['name'], app['resultsdir'])
    
    print("\n" + "=" * 60)
    print("Настройка валидаторов и ассимиляторов...")
    print("=" * 60)
    setup_daemons()
    
    # Шаблоны результатов
    for app in apps:
        run_cmd("""cd {}/templates && [ ! -f {}_out ] && cp boinc2docker_out {}_out""".format(
            PROJECT_HOME, app['name'], app['name']
        ), check=False)
    
    print("\n" + "=" * 60)
    print("✓ Все Apps созданы и настроены!")
    print("=" * 60)


if __name__ == "__main__":
    create_apps()
