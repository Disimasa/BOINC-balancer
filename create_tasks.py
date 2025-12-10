#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для создания задач (workunits) для каждого App
"""
from __future__ import print_function
import subprocess
import sys
import argparse
import os

PROJECT_HOME = "/home/boincadm/project"
CONTAINER_NAME = "server-apache-1"

# Конфигурация задач для каждого App
TASK_CONFIGS = {
    "fast_task": {
        "script": "fast_task.py",
        "default_count": 20,
        "target_nresults": 2
    },
    "medium_task": {
        "script": "medium_task.py",
        "default_count": 15,
        "target_nresults": 2
    },
    "long_task": {
        "script": "long_task.py",
        "default_count": 10,
        "target_nresults": 2
    },
    "random_task": {
        "script": "random_task.py",
        "default_count": 15,
        "target_nresults": 2
    }
}


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
            return False
        return True
    except Exception as e:
        print("Исключение при выполнении команды: {}".format(e), file=sys.stderr)
        return False


# Код скриптов задач (встроенный для надежности)
SCRIPT_CODES = {
    "fast_task.py": """
import time
start = time.time()
result = sum(i**2 for i in range(1000))
elapsed = time.time() - start
print("Fast task completed! Result: %s, Time: %.3fs" % (result, elapsed))
with open('/root/shared/results/result.txt', 'w') as f:
    f.write("Fast task result: %s\\nComputation time: %.3f seconds\\n" % (result, elapsed))
""",
    "medium_task.py": """
import time
start = time.time()
result = sum(i**2 for i in range(10000))
total = sum(i * 2 for i in range(5000))
elapsed = time.time() - start
print("Medium task completed! Result: %s, Additional: %s, Time: %.3fs" % (result, total, elapsed))
with open('/root/shared/results/result.txt', 'w') as f:
    f.write("Medium task result: %s\\nAdditional computation: %s\\nComputation time: %.3f seconds\\n" % (result, total, elapsed))
""",
    "long_task.py": """
import time
start = time.time()
max_time = 5.0
result = 0
iterations = 0
while time.time() - start < max_time:
    for i in range(10000):
        result += i**2
    iterations += 1
elapsed = time.time() - start
print("Long task completed! Result: %s, Iterations: %s, Time: %.3fs" % (result, iterations, elapsed))
with open('/root/shared/results/result.txt', 'w') as f:
    f.write("Long task result: %s\\nIterations: %s\\nComputation time: %.3f seconds\\n" % (result, iterations, elapsed))
""",
    "random_task.py": """
import time
import random
start = time.time()
target_time = random.uniform(0.5, 4.5)
result = 0
iterations = 0
while time.time() - start < target_time:
    for i in range(random.randint(1000, 10000)):
        result += i**2
    iterations += 1
elapsed = time.time() - start
print("Random task completed! Target: %.3fs, Actual: %.3fs, Result: %s, Iterations: %s" % (target_time, elapsed, result, iterations))
with open('/root/shared/results/result.txt', 'w') as f:
    f.write("Random task result: %s\\nTarget time: %.3f seconds\\nActual time: %.3f seconds\\nIterations: %s\\n" % (result, target_time, elapsed, iterations))
"""
}


def get_script_code(script_name):
    """Получить код скрипта для встраивания в команду"""
    if script_name in SCRIPT_CODES:
        code = SCRIPT_CODES[script_name].strip()
        # Экранируем для shell команды (осторожно с байтами в Python 2)
        code = code.replace('\\', '\\\\').replace('"', '\\"').replace('$', '\\$').replace('`', '\\`')
        # Заменяем переносы строк на \n
        code = code.replace('\n', '\\n')
        return code

    # Fallback: попробовать прочитать из файла
    script_path = os.path.join(os.path.dirname(__file__), "tasks", script_name)
    if os.path.exists(script_path):
        with open(script_path, 'r') as f:
            code = f.read()
        # Удаляем shebang
        if code.startswith('#!'):
            code = code.split('\n', 1)[1]
        # Экранируем
        code = code.replace('\\', '\\\\').replace('"', '\\"').replace('$', '\\$').replace('`', '\\`')
        code = code.replace('\n', '\\n')
        return code
    return None


def create_tasks_for_app(app_name, count, target_nresults, batch_id=1):
    """Создать задачи для указанного App"""
    config = TASK_CONFIGS[app_name]
    script_name = config["script"]

    print("\n" + "=" * 60)
    print("Создание {} задач для App: {}".format(count, app_name))
    print("=" * 60)

    # Получить код скрипта
    script_code = get_script_code(script_name)
    if not script_code:
        print("Ошибка: не найден скрипт {}".format(script_name), file=sys.stderr)
        return False

    for i in range(1, count + 1):
        wu_name = "{}_task_{}".format(app_name, i)

        # Создаем задачу через boinc2docker_create_work
        # --delay 0 устанавливает transition_time в текущее время (задача готова к отправке сразу)
        cmd = (
            "cd {project_home} && "
            "~/project/bin/boinc2docker_create_work.py "
            "--appname {appname} "
            "--wu_name {wu_name} "
            "--target_nresults {target_nresults} "
            "--min_quorum {min_quorum} "
            "--batch {batch_id} "
            "--priority 100 "
            'python:alpine python -c "{script_code}"'
        ).format(
            project_home=PROJECT_HOME,
            appname=app_name,
            wu_name=wu_name,
            target_nresults=target_nresults,
            min_quorum=target_nresults // 2 + 1,
            batch_id=batch_id,
            script_code=script_code
        )

        print("Создаю задачу {}/{}: {}".format(i, count, wu_name))

        success = run_command(cmd, check=False)
        if not success:
            print("Предупреждение: не удалось создать задачу {}".format(wu_name), file=sys.stderr)

    print("Завершено создание задач для {}".format(app_name))
    return True


def update_versions():
    """Обновить версии приложений"""
    print("\n" + "=" * 60)
    print("Обновляю версии приложений...")
    print("=" * 60)
    cmd = "cd {} && bin/update_versions".format(PROJECT_HOME)
    return run_command(cmd, check=False)


def create_all_tasks(counts=None, target_nresults=None):
    """Создать задачи для всех Apps"""
    if counts is None:
        counts = {}
    if target_nresults is None:
        target_nresults = {}

    for app_name, config in TASK_CONFIGS.items():
        count = counts.get(app_name, config["default_count"])
        tr = target_nresults.get(app_name, config["target_nresults"])
        create_tasks_for_app(app_name, count, tr)


def main():
    parser = argparse.ArgumentParser(
        description="Создать задачи (workunits) для Apps",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  # Создать задачи для всех Apps с настройками по умолчанию
  python create_tasks.py

  # Создать задачи для конкретного App
  python create_tasks.py --app fast_task --count 30

  # Создать задачи для всех Apps с указанным количеством
  python create_tasks.py --count 25

  # Создать задачи с разным количеством для каждого App
  python create_tasks.py --fast 20 --medium 15 --long 10 --random 15
        """
    )

    parser.add_argument("--app", choices=list(TASK_CONFIGS.keys()),
                        help="Создать задачи только для указанного App")
    parser.add_argument("--count", type=int,
                        help="Количество задач для всех Apps (или для указанного App)")
    parser.add_argument("--fast", type=int, metavar="N",
                        help="Количество задач для fast_task")
    parser.add_argument("--medium", type=int, metavar="N",
                        help="Количество задач для medium_task")
    parser.add_argument("--long", type=int, metavar="N",
                        help="Количество задач для long_task")
    parser.add_argument("--random", type=int, metavar="N",
                        help="Количество задач для random_task")
    parser.add_argument("--target-nresults", type=int, default=2,
                        help="Количество репликаций для каждой задачи (по умолчанию: 2)")

    args = parser.parse_args()

    if args.app:
        config = TASK_CONFIGS[args.app]
        count = args.count if args.count else config["default_count"]
        tr = args.target_nresults
        create_tasks_for_app(args.app, count, tr)
    else:
        counts = {}
        if args.fast is not None:
            counts["fast_task"] = args.fast
        if args.medium is not None:
            counts["medium_task"] = args.medium
        if args.long is not None:
            counts["long_task"] = args.long
        if args.random is not None:
            counts["random_task"] = args.random

        if args.count is not None and not counts:
            counts = {app: args.count for app in TASK_CONFIGS.keys()}

        target_nresults = {app: args.target_nresults for app in TASK_CONFIGS.keys()}
        create_all_tasks(counts, target_nresults)
        
        # Обновляем версии приложений после создания всех задач
        update_versions()


if __name__ == "__main__":
    main()