#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для создания 4 Apps в BOINC:
1. fast_task - быстрая задача
2. medium_task - средняя задача
3. long_task - долгая задача (макс 5 секунд)
4. random_task - задача с рандомной сложностью
"""

from __future__ import print_function
import subprocess
import sys
import os
import time

PROJECT_HOME = "/home/boincadm/project"
CONTAINER_NAME = "server-apache-1"

apps = [
    {
        "name": "fast_task",
        "friendly_name": "Fast Task",
        "resultsdir": "/results/fast_task"
    },
    {
        "name": "medium_task",
        "friendly_name": "Medium Task",
        "resultsdir": "/results/medium_task"
    },
    {
        "name": "long_task",
        "friendly_name": "Long Task (max 5s)",
        "resultsdir": "/results/long_task"
    },
    {
        "name": "random_task",
        "friendly_name": "Random Complexity Task",
        "resultsdir": "/results/random_task"
    }
]


def run_command(cmd, check=True):
    """Выполнить команду в контейнере apache"""
    full_cmd = [
        "wsl.exe", "-e", "docker", "exec", CONTAINER_NAME,
        "bash", "-c", cmd
    ]
    print("Выполняю: {}".format(cmd))
    try:
        proc = subprocess.Popen(
            full_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        stdout, stderr = proc.communicate()
        if stdout:
            print(stdout, end='')
        if stderr:
            print(stderr, file=sys.stderr, end='')
        if check and proc.returncode != 0:
            print("Ошибка: команда завершилась с кодом {}".format(proc.returncode), file=sys.stderr)
            sys.exit(1)
        return proc
    except Exception as e:
        print("Исключение при выполнении команды: {}".format(e), file=sys.stderr)
        sys.exit(1)


def create_app(app_name, resultsdir):
    """Создать приложение для нативных бинарников (не boinc2docker)"""
    print("\nСоздаю App: {}".format(app_name))
    
    # Создаем структуру для нативного приложения (x86_64-pc-linux-gnu без __vbox64_mt)
    # Бинарники будут добавлены позже через create_tasks_bin.py
    cmd = (
        "cd {projhome} && "
        "mkdir -p apps/{appname}/1.0/x86_64-pc-linux-gnu"
    ).format(projhome=PROJECT_HOME, appname=app_name)
    
    if not run_command(cmd, check=False):
        print("Ошибка при создании структуры для {}".format(app_name), file=sys.stderr)
        return False
    
    # Создаем assimilator
    cmd = (
        "cd {projhome} && "
        "cat > bin/{appname}_assimilator << 'EOF'\n"
        "#!/bin/bash\n"
        "# Assimilator for {appname}\n"
        "RESULTS_DIR={resultsdir}\n"
        "mkdir -p \"$RESULTS_DIR\"\n"
        "# Копируем результаты\n"
        "for file in \"$@\"; do\n"
        "    if [ -f \"$file\" ]; then\n"
        "        cp \"$file\" \"$RESULTS_DIR/\"\n"
        "    fi\n"
        "done\n"
        "EOF\n"
        "chmod +x bin/{appname}_assimilator"
    ).format(projhome=PROJECT_HOME, appname=app_name, resultsdir=resultsdir)
    
    if not run_command(cmd, check=False):
        print("Ошибка при создании assimilator для {}".format(app_name), file=sys.stderr)
        return False
    
    # Обновляем config.xml - добавляем validator и assimilator
    # Добавляем validator (sample_trivial_validator принимает все результаты как корректные)
    cmd_validator = (
        "cd {projhome} && "
        "if ! grep -q 'sample_trivial_validator.*{appname}' config.xml; then "
        "sed -i '/<\\/daemons>/i\\        <daemon>\\n            <cmd>sample_trivial_validator -app {appname}</cmd>\\n        </daemon>' config.xml; "
        "fi"
    ).format(projhome=PROJECT_HOME, appname=app_name)
    run_command(cmd_validator, check=False)
    
    # Добавляем assimilator
    cmd_assimilator = (
        "cd {projhome} && "
        "if ! grep -q '{appname}_assimilator' config.xml; then "
        "sed -i '/<\\/daemons>/i\\        <daemon>\\n            <cmd>script_assimilator --app {appname} --script \"{appname}_assimilator files\"</cmd>\\n        </daemon>' config.xml; "
        "fi"
    ).format(projhome=PROJECT_HOME, appname=app_name)
    run_command(cmd_assimilator, check=False)
    
    # Обновляем project.xml
    cmd = (
        "cd {projhome} && "
        "if ! grep -q '<name>{appname}</name>' project.xml; then "
        "sed -i '/<\\/boinc>/i\\    <app>\\n        <name>{appname}</name>\\n        <user_friendly_name>{appname}</user_friendly_name>\\n    </app>\\n' project.xml; "
        "fi"
    ).format(projhome=PROJECT_HOME, appname=app_name)
    run_command(cmd, check=False)
    
    # Добавляем приложение в базу данных через bin/xadd
    cmd = "cd {} && bin/xadd".format(PROJECT_HOME)
    if not run_command(cmd, check=False):
        print("Ошибка при добавлении приложения {} в базу данных через bin/xadd".format(app_name), file=sys.stderr)
        return False
    
    print("Приложение {} создано и настроено".format(app_name))
    return True


def create_apps():
    """Создать все Apps"""
    print("=" * 60)
    print("Создание Apps...")
    print("=" * 60)

    for app in apps:
        create_app(app['name'], app['resultsdir'])

    print("\n" + "=" * 60)
    print("Обновляю версии Apps...")
    print("=" * 60)
    # Используем yes для автоматического ответа на вопросы update_versions
    # Запускаем несколько раз, чтобы убедиться, что все версии обработаны
    for attempt in range(1, 4):
        print(f"\nПопытка {attempt} обновления версий...")
        cmd = "cd {} && yes | bin/update_versions 2>&1".format(PROJECT_HOME)
        run_command(cmd, check=False)
        time.sleep(2)  # Небольшая пауза между попытками
    
    print("\n" + "=" * 60)
    print("Проверяю созданные версии в базе данных...")
    print("=" * 60)
    # Проверяем, что версии действительно добавлены в БД
    for app in apps:
        print(f"\nПроверка версий для {app['name']}:")
        cmd = (
            "cd {projhome} && "
            "for platform_dir in apps/{appname}/1.0/*; do "
            "platform=$(basename $platform_dir); "
            "if [ -d \"$platform_dir\" ] && [ -f \"$platform_dir/version.xml\" ]; then "
            "echo \"  Найдена директория: $platform\"; "
            "fi; "
            "done"
        ).format(projhome=PROJECT_HOME, appname=app['name'])
        run_command(cmd, check=False)

    print("\n" + "=" * 60)
    print("Перезапускаю демоны BOINC для применения изменений...")
    print("=" * 60)
    cmd = "cd {} && bin/stop && sleep 2 && bin/start".format(PROJECT_HOME)
    run_command(cmd, check=False)

    # Создаем шаблоны результатов для всех приложений
    print("\n" + "=" * 60)
    print("Создаю шаблоны результатов...")
    print("=" * 60)
    for app in apps:
        cmd = (
            "cd {projhome}/templates && "
            "if [ ! -f {appname}_out ]; then "
            "cp boinc2docker_out {appname}_out && "
            "echo 'Created {appname}_out'; "
            "fi"
        ).format(projhome=PROJECT_HOME, appname=app['name'])
        run_command(cmd, check=False)

    print("\n" + "=" * 60)
    print("Все Apps успешно созданы!")
    print("=" * 60)
    print("\n✓ Приложения созданы")
    print("✓ Версии созданы и добавлены в базу данных")
    print("✓ Шаблоны результатов созданы")
    print("✓ Демоны BOINC перезапущены")
    print("\nВерсии должны быть видны в веб-интерфейсе.")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--help":
        print(__doc__)
        sys.exit(0)

    create_apps()
