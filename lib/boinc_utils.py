#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Утилиты для работы с BOINC контейнером.
"""
import sys
import time
from lib.utils import run_command, PROJECT_HOME
from lib.daemons import start_all_daemons


def trigger_feeder_update():
    run_command(f"touch {PROJECT_HOME}/reread_db", check=False)


def restart_feeder():
    """Перезапустить feeder для пересчета распределения слотов.
    
    ВАЖНО: После обновления весов нужно перезапустить feeder,
    так как weighted_interleave (распределение слотов) вызывается
    только при старте и не пересчитывается после reread_db.
    
    Используем SIGHUP для корректной остановки feeder, затем запускаем заново.
    """
    # Находим PID процесса feeder (точное совпадение команды)
    cmd = "ps aux | grep '[f]eeder -d' | awk '{print $2}'"
    stdout, success = run_command(cmd, check=False, capture_output=True)
    
    if success and stdout:
        pid = stdout.strip().split('\n')[0].strip()
        if pid and pid.isdigit():
            # Отправляем SIGHUP для корректной остановки feeder
            run_command(f"kill -HUP {pid}", check=False)
            time.sleep(2)
            
            # Запускаем feeder заново
            run_command(f"cd {PROJECT_HOME} && nohup bin/feeder -d 3 --allapps --priority_order --sleep_interval 1 > logs/feeder.log 2>&1 &", check=False)
            time.sleep(3)
            
            # Проверяем, что feeder запустился
            cmd_check = "ps aux | grep '[f]eeder -d' | wc -l"
            stdout_check, success_check = run_command(cmd_check, check=False, capture_output=True)
            
            if success_check and stdout_check and int(stdout_check.strip()) > 0:
                return True
            else:
                print("⚠ Feeder не запустился после перезапуска", file=sys.stderr)
                return False
    
    print("⚠ Процесс feeder не найден", file=sys.stderr)
    return False


def ensure_daemons_running():
    """Убедиться, что все демоны (валидаторы и ассимиляторы) запущены.
    
    Перезапускает упавшие демоны после перезапуска feeder.
    """
    start_all_daemons()

