#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для просмотра количества задач каждого типа в очереди feeder (shared memory).

Shared memory в BOINC:
- Создается процессом feeder
- Содержит массив задач (work items), готовых к отправке клиентам
- Scheduler читает из неё и отправляет задачи клиентам
- Обновляется feeder'ом при добавлении новых задач из БД
"""
import sys
import re
from collections import defaultdict
from lib.utils import run_command, PROJECT_HOME


def get_queue_shares_from_shmem():
    """
    Получить долю занятых слотов shared memory (workarray) по приложениям.
    Возвращает словарь {app_name: share}, где share в [0,1].
    Если не удалось получить/распарсить данные, возвращает пустой словарь.
    """
    cmd = f"cd {PROJECT_HOME} && bin/show_shmem"
    stdout, success = run_command(cmd, check=False, capture_output=True)
    if not success or not stdout:
        return {}

    app_counts = defaultdict(int)
    total_occupied = 0
    lines = stdout.split('\n')
    in_jobs_section = False

    for line in lines:
        # Ищем начало секции Jobs
        if 'slot' in line.lower() and 'app' in line.lower() and 'wu id' in line.lower():
            in_jobs_section = True
            continue
        if not in_jobs_section:
            continue
        # Пропускаем заголовки/разделители
        if 'slot' in line.lower() and 'app' in line.lower() and 'wu id' in line.lower():
            continue
        if line.strip().startswith('-'):
            continue
        if not line.strip():
            continue
        # Пустой слот: "   1: ---"
        parts = line.split()
        if len(parts) < 2:
            continue
        if parts[0].endswith(':') and parts[1] == '---':
            # пустой слот, не учитываем в занятых
            continue
        # Ожидаемый формат занятого слота: "<slot> <app_name> <WU_ID> ..."
        match = re.match(r'\s*\d+\s+(\w+)\s+\d+', line)
        if match:
            app_name = match.group(1)
            if app_name != '---':
                app_counts[app_name] += 1
                total_occupied += 1

    if total_occupied == 0:
        return {}

    return {name: count / float(total_occupied) for name, count in app_counts.items()}

def get_weights_from_shmem():
    """Получить веса приложений из shared memory через show_shmem."""
    cmd = f"cd {PROJECT_HOME} && bin/show_shmem | grep '^id:'"
    stdout, success = run_command(cmd, check=False, capture_output=True)
    
    if not success or not stdout:
        return {}
    
    weights = {}
    target_apps = {'fast_task', 'medium_task', 'long_task', 'random_task'}
    
    for line in stdout.strip().split('\n'):
        if 'name:' not in line or 'weight:' not in line:
            continue
        
        parts = line.split()
        try:
            name_idx = parts.index('name:')
            weight_idx = parts.index('weight:')
            
            if name_idx + 1 < len(parts) and weight_idx + 1 < len(parts):
                app_name = parts[name_idx + 1]
                if app_name in target_apps:
                    weight_str = parts[weight_idx + 1]
                    weight = float(weight_str)
                    weights[app_name] = weight
        except (ValueError, IndexError):
            continue
    
    return weights

def show_feeder_queue_count():
    """Показать количество задач каждого типа в очереди feeder."""
    print("=" * 80)
    print("ВЕСА ПРИЛОЖЕНИЙ В FEEDER (SHARED MEMORY)")
    print("=" * 80)
    
    shmem_weights = get_weights_from_shmem()
    if shmem_weights:
        print(f"\n{'Приложение':<20} {'Вес':<15}")
        print("-" * 35)
        for app_name in sorted(shmem_weights.keys()):
            print(f"{app_name:<20} {shmem_weights[app_name]:<15.2f}")
    else:
        print("\n⚠ Не удалось получить веса из shared memory")
    
    print("\n" + "=" * 80)
    print("КОЛИЧЕСТВО ЗАДАЧ В ОЧЕРЕДИ FEEDER (SHARED MEMORY)")
    print("=" * 80)
    
    cmd = f"cd {PROJECT_HOME} && bin/show_shmem"
    stdout, success = run_command(cmd, check=False, capture_output=True)
    
    if not success:
        print("✗ Ошибка при выполнении show_shmem", file=sys.stderr)
        print("Возможно, feeder не запущен или shared memory не создана", file=sys.stderr)
        return False
    
    if not stdout:
        print("Нет вывода от show_shmem")
        return False
    
    # Парсим вывод show_shmem
    # Ищем строки вида: "   3    long_task        384        384..."
    app_counts = defaultdict(int)
    total_slots = 0
    empty_slots = 0
    
    lines = stdout.split('\n')
    in_jobs_section = False
    
    for line in lines:
        # Ищем начало секции Jobs
        if 'slot' in line.lower() and 'app' in line.lower() and 'WU ID' in line:
            in_jobs_section = True
            continue
        
        if not in_jobs_section:
            continue
        
        # Пропускаем повторяющиеся заголовки
        if 'slot' in line.lower() and 'app' in line.lower() and 'WU ID' in line:
            continue
        
        # Пропускаем разделители
        if line.strip().startswith('-'):
            continue
        
        # Пустой слот: "   1: ---" или "  86: ---"
        if re.match(r'\s*\d+:\s+---', line):
            empty_slots += 1
            total_slots += 1
            continue
        
        # Строка с задачей: "   0    long_task       1698       1698..."
        # Формат: номер_слота (без двоеточия), имя_приложения, WU ID, result ID, ...
        match = re.match(r'\s*\d+\s+(\w+)\s+\d+', line)
        if match:
            app_name = match.group(1)
            app_counts[app_name] += 1
            total_slots += 1
    
    if not app_counts and empty_slots == 0:
        print("Не удалось распарсить вывод show_shmem")
        return False
    
    print(f"\nВсего слотов: {total_slots}")
    print(f"Занято: {total_slots - empty_slots}")
    print(f"Пусто: {empty_slots}")
    
    if app_counts:
        print(f"\n{'Приложение':<20} {'Количество':<15} {'% от занятых':<15}")
        print("-" * 50)
        total_occupied = sum(app_counts.values())
        for app_name in sorted(app_counts.keys()):
            count = app_counts[app_name]
            percentage = (count / total_occupied * 100) if total_occupied > 0 else 0
            print(f"{app_name:<20} {count:<15} {percentage:<15.1f}%")
    else:
        print("\nНет задач в очереди")
    
    return True


if __name__ == "__main__":
    show_feeder_queue_count()

