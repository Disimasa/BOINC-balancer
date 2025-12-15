#!/usr/bin/env python3
import subprocess
import sys
import os
import shutil
from pathlib import Path

PROJECT_HOME = "/home/boincadm/project"
CONTAINER_NAME = "server-apache-1"
SCRIPT_DIR = Path(__file__).parent.parent.absolute()
SERVER_DIR = SCRIPT_DIR

def get_docker_cmd():
    # if shutil.which("wsl.exe"):
    #     return ["wsl.exe", "-e", "docker"]
    return ["docker"]


def run_local_command(cmd, check=True, capture_output=False, cwd=None, shell=False, timeout=None, stdin=None, input=None, **kwargs):
    if isinstance(cmd, str):
        if shell:
            full_cmd = cmd
        else:
            full_cmd = cmd.split()
    else:
        full_cmd = list(cmd)
    
    if full_cmd and full_cmd[0] == "docker":
        docker_cmd = get_docker_cmd()
        full_cmd = docker_cmd + full_cmd[1:]
    
    run_kwargs = {
        "check": check,
        "capture_output": capture_output,
        "text": True,
        "cwd": cwd,
        "shell": shell,
        "timeout": timeout,
        **kwargs
    }
    
    if stdin is not None:
        run_kwargs["stdin"] = stdin
    if input is not None:
        run_kwargs["input"] = input
        if stdin is None:
            run_kwargs["stdin"] = subprocess.PIPE
    
    try:
        result = subprocess.run(full_cmd, **run_kwargs)
        return result
    except subprocess.TimeoutExpired as e:
        if capture_output:
            return subprocess.CompletedProcess(full_cmd, -1, "", "Timeout")
        raise
    except subprocess.CalledProcessError as e:
        if not check:
            return e
        raise
    except Exception as e:
        if not check:
            return subprocess.CompletedProcess(full_cmd, -1, "", str(e))
        raise


def run_command(cmd, check=True, capture_output=False, cwd=None, shell=False):
    docker_cmd = get_docker_cmd()
    if isinstance(cmd, str) and not shell:
        full_cmd = docker_cmd + ["exec", CONTAINER_NAME, "bash", "-c", cmd]
    elif isinstance(cmd, str) and shell:
        full_cmd = cmd
    else:
        full_cmd = docker_cmd + ["exec", CONTAINER_NAME] + list(cmd)
    
    try:
        result = subprocess.run(
            full_cmd,
            capture_output=True,
            text=True,
            timeout=60,
            cwd=cwd,
            shell=shell
        )
        
        stdout = result.stdout if result.stdout else ""
        stderr = result.stderr if result.stderr else ""
        success = result.returncode == 0
        
        if not capture_output:
            if stdout:
                print(stdout, end='', flush=True)
            if stderr:
                print(stderr, file=sys.stderr, end='', flush=True)
        
        if check and not success:
            if not capture_output:
                print(f"\n✗ Ошибка: команда завершилась с кодом {result.returncode}", file=sys.stderr)
            return "", False
        
        return stdout.strip(), success
    except subprocess.TimeoutExpired:
        if not capture_output:
            print("✗ Ошибка: команда превысила время ожидания", file=sys.stderr)
        return "", False
    except Exception as e:
        if not capture_output:
            print(f"✗ Исключение при выполнении команды: {e}", file=sys.stderr)
        return "", False


def load_env_file(env_path=None):
    if env_path is None:
        env_path = SCRIPT_DIR / '.env'
    env_vars = {}
    if env_path.exists():
        with open(env_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip().strip('"').strip("'")
                    env_vars[key] = value
    return env_vars


def check_file_exists(file_path):
    cmd = f"test -f {file_path}"
    stdout, success = run_command(cmd, check=False, capture_output=True)
    return success

