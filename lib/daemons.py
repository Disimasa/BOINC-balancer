#!/usr/bin/env python3
import sys
import time
from lib.utils import run_command, PROJECT_HOME

APPS = ["fast_task", "medium_task", "long_task", "random_task"]


def check_validator_running(app_name):
    cmd = f"ps aux | grep '[s]ample_trivial_validator -app {app_name}'"
    stdout, success = run_command(f"cd {PROJECT_HOME} && {cmd}", check=False, capture_output=True)
    return bool(stdout and stdout.strip())


def check_assimilator_running(app_name):
    cmd = f"ps aux | grep '[s]cript_assimilator.*--app {app_name}'"
    stdout, success = run_command(f"cd {PROJECT_HOME} && {cmd}", check=False, capture_output=True)
    return bool(stdout and stdout.strip())


def start_validator(app_name):
    if check_validator_running(app_name):
        return True
    
    cmd = f"mkdir -p logs && nohup bin/sample_trivial_validator -app {app_name} > logs/validator_{app_name}.log 2>&1 &"
    if run_command(f"cd {PROJECT_HOME} && {cmd}", check=False):
        time.sleep(1)
        if check_validator_running(app_name):
            return True
        else:
            print(f"✗ Валидатор для {app_name} не запустился", file=sys.stderr)
            return False
    else:
        print(f"✗ Ошибка запуска валидатора для {app_name}", file=sys.stderr)
        return False


def start_assimilator(app_name):
    if check_assimilator_running(app_name):
        return True
    
    run_command(f"cd {PROJECT_HOME} && mkdir -p ../bin && ln -sf {PROJECT_HOME}/bin/{app_name}_assimilator ../bin/{app_name}_assimilator", check=False)
    
    check_script_cmd = f"test -f bin/{app_name}_assimilator && echo 'exists' || echo 'missing'"
    stdout, _ = run_command(f"cd {PROJECT_HOME} && {check_script_cmd}", check=False, capture_output=True)
    if "missing" in stdout:
        print(f"⚠ Предупреждение: скрипт bin/{app_name}_assimilator не найден", file=sys.stderr)
    
    cmd = f"mkdir -p logs && PATH={PROJECT_HOME}/bin:$PATH nohup bin/script_assimilator --app {app_name} --script \"{app_name}_assimilator files\" > logs/assimilator_{app_name}.log 2>&1 &"
    if run_command(f"cd {PROJECT_HOME} && {cmd}", check=False):
        time.sleep(1)
        if check_assimilator_running(app_name):
            return True
        else:
            print(f"✗ Ассимилятор для {app_name} не запустился", file=sys.stderr)
            stdout, _ = run_command(f"cd {PROJECT_HOME} && tail -5 logs/assimilator_{app_name}.log 2>/dev/null || echo 'Log not found'", check=False, capture_output=True)
            if stdout:
                print(f"  Лог: {stdout}", file=sys.stderr)
            return False
    else:
        print(f"✗ Ошибка запуска ассимилятора для {app_name}", file=sys.stderr)
        return False


def start_all_daemons():
    success = True
    
    for app_name in APPS:
        if not start_validator(app_name):
            success = False
    
    for app_name in APPS:
        if not start_assimilator(app_name):
            success = False
    
    if not success:
        print("⚠ Некоторые валидаторы или ассимиляторы не запустились", file=sys.stderr)
    
    return success

