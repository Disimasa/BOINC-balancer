#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Утилиты для работы с BOINC контейнером.
"""
import subprocess
import sys

PROJECT_HOME = "/home/boincadm/project"
CONTAINER_NAME = "server-apache-1"


def run_command(cmd, check=True, capture_output=False):
    """Выполнить команду в контейнере apache и вывести stdout/stderr."""
    if isinstance(cmd, str):
        full_cmd = ["wsl.exe", "-e", "docker", "exec", CONTAINER_NAME, "bash", "-c", cmd]
    else:
        full_cmd = ["wsl.exe", "-e", "docker", "exec", CONTAINER_NAME] + cmd
    
    try:
        proc = subprocess.Popen(
            full_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        stdout, stderr = proc.communicate()
        
        if check and proc.returncode != 0:
            if not capture_output:
                print(f"✗ Ошибка: команда завершилась с кодом {proc.returncode}", file=sys.stderr)
                if stderr:
                    print(stderr, file=sys.stderr)
            if capture_output:
                return ""
            return None
        
        if capture_output:
            return stdout.strip() if stdout else ""
        return True
    except Exception as e:
        if not capture_output:
            print(f"✗ Исключение при выполнении команды: {e}", file=sys.stderr)
        if capture_output:
            return ""
        return None

