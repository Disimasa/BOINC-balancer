#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для сбора базовой статистики работы BOINC без балансировщика.

Процесс:
1. Запускает pipeline.py для чистого старта
2. Создает большое количество тасок (чтобы их хватило на весь эксперимент)
3. Ждет 2 минуты
4. Собирает статистику по таскам и клиентам
"""

from __future__ import print_function
import subprocess
import sys
import time
import json
from pathlib import Path
from datetime import datetime

# Импортируем функции из библиотеки
from lib.boinc_utils import run_command
from lib.statistics import get_completed_task_statistics, get_completed_client_statistics

SCRIPT_DIR = Path(__file__).parent.absolute()
PROJECT_HOME = "/home/boincadm/project"
CONTAINER_NAME = "server-apache-1"

# Количество тасок создается в pipeline.py (200 на каждое приложение)


def run_python_script(script_name, *args):
    """Запустить Python скрипт"""
    script_path = SCRIPT_DIR / script_name
    if not script_path.exists():
        print(f"✗ Ошибка: скрипт {script_name} не найден", file=sys.stderr)
        return False
    
    cmd = [sys.executable, str(script_path)] + list(args)
    full_cmd = ["wsl.exe", "-e"] + cmd
    
    print(f"\n{'='*80}")
    print(f"Запускаю: {' '.join(cmd)}")
    print(f"{'='*80}")
    
    try:
        proc = subprocess.Popen(
            full_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1,
            cwd=SCRIPT_DIR
        )
        
        for line in proc.stdout:
            print(line, end='', flush=True)
        
        proc.wait()
        return proc.returncode == 0
    except Exception as e:
        print(f"✗ Исключение: {e}", file=sys.stderr)
        return False


def step_cleanup_and_setup(balance_hosts=False):
    """Шаг 1: Запуск pipeline.py для чистого старта"""
    print("\n" + "="*80)
    print("ШАГ 1: Запуск pipeline.py для чистого старта")
    print("="*80)
    
    args = []
    if balance_hosts:
        args.append("--balance-hosts")
    return run_python_script("pipeline.py", *args)


# Убрано: создание тасок теперь происходит в pipeline.py


def step_wait():
    """Шаг 3: Ожидание 2 минуты"""
    print("\n" + "="*80)
    print("ШАГ 3: Ожидание 2 минуты для выполнения задач")
    print("="*80)
    
    wait_seconds = 600
    for i in range(wait_seconds):
        if i % 10 == 0:
            remaining = wait_seconds - i
            print(f"Ожидание... осталось {remaining} секунд")
        time.sleep(1)
    
    print("✓ Ожидание завершено")
    return True


# Функции get_task_statistics и get_client_statistics теперь импортируются из lib.statistics


def print_task_statistics(stats):
    """Вывести статистику по завершенным таскам"""
    print("\n" + "="*80)
    print("СТАТИСТИКА ПО ЗАВЕРШЕННЫМ ТАСКАМ (ПРИЛОЖЕНИЯМ)")
    print("="*80)
    
    if not stats:
        print("Нет данных о завершенных задачах")
        return
    
    print(f"\n{'Приложение':<15} {'Вес':<8} {'Завершено WU':<15} {'Завершено результатов':<22}")
    print("-" * 80)
    
    for stat in stats:
        print(f"{stat.get('app_name', 'N/A'):<15} "
              f"{stat.get('app_weight', 0):<8.2f} "
              f"{stat.get('completed_workunits', 0):<15} "
              f"{stat.get('completed_results', 0):<22}")
    
    print("\n" + "="*80)
    print("МЕТРИКИ ВЫПОЛНЕНИЯ (для завершенных задач)")
    print("="*80)
    
    print(f"\n{'Приложение':<15} {'Ср. время (сек)':<18} {'Ср. CPU (сек)':<18} {'Ср. кредит':<15} {'Всего кредит':<15}")
    print("-" * 80)
    
    for stat in stats:
        print(f"{stat.get('app_name', 'N/A'):<15} "
              f"{stat.get('avg_elapsed_time', 0):<18.4f} "
              f"{stat.get('avg_cpu_time', 0):<18.4f} "
              f"{stat.get('avg_credit', 0):<15.4f} "
              f"{stat.get('total_credit', 0):<15.4f}")
    
    print("\n" + "="*80)
    print("ВРЕМЯ В ОЧЕРЕДИ И ВЫПОЛНЕНИЯ (для завершенных задач)")
    print("="*80)
    
    print(f"\n{'Приложение':<15} {'Ср. время в очереди (сек)':<28} {'Ср. время выполнения (сек)':<30}")
    print("-" * 80)
    
    for stat in stats:
        print(f"{stat.get('app_name', 'N/A'):<15} "
              f"{stat.get('avg_queue_time', 0):<28.4f} "
              f"{stat.get('avg_execution_time', 0):<30.4f}")


def print_client_statistics(stats):
    """Вывести статистику по клиентам (только для завершенных задач)"""
    print("\n" + "="*80)
    print("СТАТИСТИКА ПО КЛИЕНТАМ (только завершенные задачи)")
    print("="*80)
    
    if not stats:
        print("Нет данных о завершенных задачах")
        return
    
    print(f"\n{'ID':<6} {'Имя':<20} {'Завершено':<12} {'Простой (сек)':<15} {'Последний RPC (сек)':<20}")
    print("-" * 80)
    
    for stat in stats:
        host_name = stat.get('host_name', f"host_{stat.get('host_id', 'N/A')}")
        if len(host_name) > 18:
            host_name = host_name[:15] + "..."
        
        print(f"{stat.get('host_id', 0):<6} "
              f"{host_name:<20} "
              f"{stat.get('completed_results', 0):<12} "
              f"{stat.get('idle_time_seconds', 0):<15} "
              f"{stat.get('time_since_last_rpc_seconds', 0):<20}")
    
    print("\n" + "="*80)
    print("РАСПРЕДЕЛЕНИЕ ЗАВЕРШЕННЫХ ЗАДАЧ ПО ПРИЛОЖЕНИЯМ (для каждого клиента)")
    print("="*80)
    
    print(f"\n{'ID':<6} {'Имя':<20} {'fast_task':<12} {'medium_task':<14} {'long_task':<12} {'random_task':<14}")
    print("-" * 80)
    
    for stat in stats:
        host_name = stat.get('host_name', f"host_{stat.get('host_id', 'N/A')}")
        if len(host_name) > 18:
            host_name = host_name[:15] + "..."
        
        print(f"{stat.get('host_id', 0):<6} "
              f"{host_name:<20} "
              f"{stat.get('fast_task_completed', 0):<12} "
              f"{stat.get('medium_task_completed', 0):<14} "
              f"{stat.get('long_task_completed', 0):<12} "
              f"{stat.get('random_task_completed', 0):<14}")


def save_statistics_to_file(task_stats, client_stats, with_balancer=False, balancer_config=None):
    """Сохранить статистику в JSON файл"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    prefix = "balanced_stats" if with_balancer else "baseline_stats"
    filename = SCRIPT_DIR / f"{prefix}_{timestamp}.json"
    
    data = {
        "timestamp": datetime.now().isoformat(),
        "with_balancer": with_balancer,
        "balancer_config": balancer_config or {},
        "task_statistics": task_stats,
        "client_statistics": client_stats
    }
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    print(f"\n✓ Статистика сохранена в файл: {filename}")
    return filename


def run_balancer_loop(interval=10, smoothing=0.3):
    """Запустить балансировщик в фоновом режиме"""
    import threading
    from pathlib import Path
    
    balancer_script = SCRIPT_DIR / "dynamic_balancer.py"
    if not balancer_script.exists():
        print(f"✗ Ошибка: скрипт {balancer_script} не найден", file=sys.stderr)
        return None
    
    def balancer_thread():
        """Поток для запуска балансировщика"""
        cmd = [sys.executable, str(balancer_script), "--loop", "--interval", str(interval), 
               "--smoothing", str(smoothing), "--quiet"]
        full_cmd = ["wsl.exe", "-e"] + cmd
        
        try:
            proc = subprocess.Popen(
                full_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                cwd=SCRIPT_DIR
            )
            # Ждем завершения процесса
            proc.wait()
        except Exception as e:
            print(f"✗ Ошибка в балансировщике: {e}", file=sys.stderr)
    
    thread = threading.Thread(target=balancer_thread, daemon=True)
    thread.start()
    return thread


def main():
    """Основная функция"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Сбор базовой статистики работы BOINC",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--with-balancer", action="store_true",
                       help="Запустить с балансировщиком нагрузки (app.weight)")
    parser.add_argument("--balance-hosts", action="store_true",
                       help="Балансировать задачи между хостами при создании")
    parser.add_argument("--balancer-interval", type=int, default=10,
                       help="Интервал балансировки в секундах (по умолчанию 10)")
    parser.add_argument("--balancer-smoothing", type=float, default=0.3,
                       help="Коэффициент сглаживания балансировщика (по умолчанию 0.3)")
    
    args = parser.parse_args()
    
    mode_parts = []
    if args.with_balancer:
        mode_parts.append("С БАЛАНСИРОВЩИКОМ (app.weight)")
    if args.balance_hosts:
        mode_parts.append("С БАЛАНСИРОВКОЙ ХОСТОВ")
    mode = " | ".join(mode_parts) if mode_parts else "БЕЗ БАЛАНСИРОВКИ"
    
    print("="*80)
    print(f"СБОР БАЗОВОЙ СТАТИСТИКИ: {mode}")
    print("="*80)

    balancer_thread = None
    if args.with_balancer:
        print("\n" + "=" * 80)
        print("ЗАПУСК БАЛАНСИРОВЩИКА")
        print("=" * 80)
        print(f"Интервал: {args.balancer_interval} секунд")
        print(f"Сглаживание: {args.balancer_smoothing}")
        balancer_thread = run_balancer_loop(
            interval=args.balancer_interval,
            smoothing=args.balancer_smoothing
        )
        if balancer_thread:
            print("  ✓ Балансировщик запущен в фоновом режиме")
            # Даем балансировщику время на первую итерацию
            time.sleep(2)

    # Шаг 1: Чистый старт (pipeline.py уже создает таски)
    if not step_cleanup_and_setup(balance_hosts=args.balance_hosts):
        print("✗ Ошибка при запуске pipeline.py", file=sys.stderr)
        return 1
    
    # Даем время задачам создаться в БД после pipeline
    # print("\nОжидание создания задач в БД...")
    # time.sleep(3)
    
    # Запускаем балансировщик, если нужно
    
    # Шаг 2: Ожидание
    step_wait()
    
    # Шаг 3: Сбор статистики
    print("\n" + "="*80)
    print("ШАГ 4: Сбор статистики")
    print("="*80)
    
    task_stats = get_completed_task_statistics()
    client_stats = get_completed_client_statistics()
    
    # Вывод статистики
    print_task_statistics(task_stats)
    print_client_statistics(client_stats)
    
    # Сохранение в файл
    if task_stats is not None and client_stats is not None:
        balancer_config = None
        if args.with_balancer:
            balancer_config = {
                "interval": args.balancer_interval,
                "smoothing": args.balancer_smoothing
            }
        filename = save_statistics_to_file(
            task_stats, 
            client_stats, 
            with_balancer=args.with_balancer,
            balancer_config=balancer_config
        )
    
    print("\n" + "="*80)
    print("✓ Сбор статистики завершен")
    print("="*80)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

