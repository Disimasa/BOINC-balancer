#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Проверка весов в БД и shared memory."""
import sys
from lib.apps import get_current_weights
from lib.boinc_utils import trigger_feeder_update
from lib.utils import run_command, PROJECT_HOME

def get_weights_from_shmem():
    """Получить веса из shared memory."""
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

if __name__ == "__main__":
    print("=" * 80)
    print("ПРОВЕРКА ВЕСОВ")
    print("=" * 80)
    
    print("\n1. Веса в БД:")
    db_weights = get_current_weights()
    for app_name in sorted(db_weights.keys()):
        print(f"   {app_name}: {db_weights[app_name]}")
    
    print("\n2. Веса в shared memory:")
    shmem_weights = get_weights_from_shmem()
    for app_name in sorted(shmem_weights.keys()):
        print(f"   {app_name}: {shmem_weights[app_name]}")
    
    print("\n3. Сравнение:")
    all_match = True
    for app_name in set(db_weights.keys()) | set(shmem_weights.keys()):
        db_w = db_weights.get(app_name, 0)
        shmem_w = shmem_weights.get(app_name, 0)
        match = abs(db_w - shmem_w) < 0.01
        status = "✓" if match else "✗"
        print(f"   {status} {app_name}: БД={db_w}, shmem={shmem_w}")
        if not match:
            all_match = False
    
    if not all_match:
        print("\n4. Веса не совпадают! Создаю триггер reread_db...")
        trigger_feeder_update()
        print("   ✓ Триггер создан. Подождите 2-3 секунды и запустите скрипт снова.")
    else:
        print("\n✓ Веса совпадают")



