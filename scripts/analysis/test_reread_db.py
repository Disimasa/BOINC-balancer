#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Тест для проверки обновления shared memory после reread_db.
"""
import sys
import time
from lib.utils import run_command, PROJECT_HOME
from lib.apps import get_current_weights, update_weights
from lib.boinc_utils import trigger_feeder_update

def get_weights_from_shmem():
    """Получить веса приложений из shared memory через show_shmem."""
    cmd = f"cd {PROJECT_HOME} && bin/show_shmem | grep '^id:'"
    stdout, success = run_command(cmd, check=False, capture_output=True)
    
    if not success or not stdout:
        return {}
    
    weights = {}
    target_apps = {'fast_task', 'medium_task', 'long_task', 'random_task'}
    
    for line in stdout.strip().split('\n'):
        # Формат: "id: 2 name: fast_task hr: 0 weight: 1.00 ..."
        if 'name:' not in line or 'weight:' not in line:
            continue
        
        parts = line.split()
        try:
            # Ищем индекс 'name:' и 'weight:'
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


def test_reread_db():
    """Тест обновления shared memory через reread_db."""
    print("=" * 80)
    print("ТЕСТ ОБНОВЛЕНИЯ SHARED MEMORY ЧЕРЕЗ reread_db")
    print("=" * 80)
    
    # 1. Получаем текущие веса из БД
    print("\n1. Текущие веса в БД:")
    db_weights = get_current_weights()
    for app_name, weight in sorted(db_weights.items()):
        print(f"   {app_name}: {weight}")
    
    # 2. Получаем текущие веса из shared memory
    print("\n2. Текущие веса в shared memory:")
    shmem_weights = get_weights_from_shmem()
    if not shmem_weights:
        print("   ⚠ Не удалось получить веса из shared memory")
        return False
    
    for app_name, weight in sorted(shmem_weights.items()):
        print(f"   {app_name}: {weight}")
    
    # 3. Проверяем, совпадают ли веса
    print("\n3. Сравнение весов:")
    weights_match = True
    for app_name in set(db_weights.keys()) | set(shmem_weights.keys()):
        db_w = db_weights.get(app_name, 0)
        shmem_w = shmem_weights.get(app_name, 0)
        match = abs(db_w - shmem_w) < 0.01
        status = "✓" if match else "✗"
        print(f"   {status} {app_name}: БД={db_w}, shmem={shmem_w}")
        if not match:
            weights_match = False
    
    if weights_match:
        print("\n   ✓ Веса совпадают, нужно изменить веса для теста")
    
    # 4. Изменяем веса в БД (увеличиваем long_task на 1000)
    print("\n4. Изменяем веса в БД (long_task + 1000):")
    new_weights = db_weights.copy()
    if 'long_task' in new_weights:
        old_weight = new_weights['long_task']
        new_weights['long_task'] = old_weight + 1000
        print(f"   long_task: {old_weight} -> {new_weights['long_task']}")
    else:
        new_weights['long_task'] = 1000
        print(f"   long_task: создан с весом {new_weights['long_task']}")
    
    success = update_weights(new_weights)
    if not success:
        print("   ✗ Ошибка при обновлении весов в БД")
        return False
    
    print("   ✓ Веса обновлены в БД")
    
    # 5. Проверяем, что веса в БД изменились
    print("\n5. Проверяем веса в БД после обновления:")
    db_weights_after = get_current_weights()
    for app_name, weight in sorted(db_weights_after.items()):
        print(f"   {app_name}: {weight}")
    
    if 'long_task' in db_weights_after:
        if abs(db_weights_after['long_task'] - new_weights['long_task']) < 0.01:
            print("   ✓ Веса в БД обновлены корректно")
        else:
            print("   ✗ Веса в БД не обновились")
            return False
    
    # 6. Проверяем, что веса в shared memory еще старые
    print("\n6. Проверяем веса в shared memory (должны быть старые):")
    shmem_weights_before = get_weights_from_shmem()
    for app_name, weight in sorted(shmem_weights_before.items()):
        print(f"   {app_name}: {weight}")
    
    if 'long_task' in shmem_weights_before:
        if abs(shmem_weights_before['long_task'] - new_weights['long_task']) < 0.01:
            print("   ⚠ Веса в shared memory уже обновлены (возможно, feeder уже обработал)")
        else:
            print("   ✓ Веса в shared memory еще старые (как и ожидалось)")
    
    # 7. Создаем триггер reread_db
    print("\n7. Создаем триггер reread_db:")
    trigger_feeder_update()
    print("   ✓ Триггер создан")
    
    # 8. Ждем обработки (feeder проверяет триггер в каждой итерации)
    print("\n8. Ожидание обработки триггера feeder'ом (до 10 секунд):")
    max_wait = 10
    wait_interval = 0.5
    waited = 0
    updated = False
    
    while waited < max_wait:
        time.sleep(wait_interval)
        waited += wait_interval
        
        # Проверяем, удален ли файл триггера
        cmd = f"test -f {PROJECT_HOME}/reread_db"
        _, exists = run_command(cmd, check=False, capture_output=True)
        
        if not exists:
            print(f"   ✓ Триггер обработан (через {waited:.1f} сек)")
            updated = True
            break
        else:
            print(f"   ... ждем ({waited:.1f} сек)")
    
    if not updated:
        print(f"   ⚠ Триггер не обработан за {max_wait} секунд")
        print("   (возможно, feeder не запущен или работает медленно)")
    
    # 9. Проверяем веса в shared memory после обработки
    print("\n9. Проверяем веса в shared memory после обработки:")
    time.sleep(1)  # Даем время на обновление
    shmem_weights_after = get_weights_from_shmem()
    
    if not shmem_weights_after:
        print("   ✗ Не удалось получить веса из shared memory")
        return False
    
    for app_name, weight in sorted(shmem_weights_after.items()):
        print(f"   {app_name}: {weight}")
    
    # 10. Сравниваем веса
    print("\n10. Финальное сравнение:")
    test_passed = True
    for app_name in set(db_weights_after.keys()) | set(shmem_weights_after.keys()):
        db_w = db_weights_after.get(app_name, 0)
        shmem_w = shmem_weights_after.get(app_name, 0)
        match = abs(db_w - shmem_w) < 0.01
        status = "✓" if match else "✗"
        print(f"   {status} {app_name}: БД={db_w}, shmem={shmem_w}")
        if not match:
            test_passed = False
    
    # 11. Восстанавливаем исходные веса
    print("\n11. Восстанавливаем исходные веса:")
    restore_success = update_weights(db_weights)
    if restore_success:
        print("   ✓ Исходные веса восстановлены")
        trigger_feeder_update()
        print("   ✓ Триггер создан для восстановления")
    else:
        print("   ⚠ Не удалось восстановить исходные веса")
    
    # Итог
    print("\n" + "=" * 80)
    if test_passed:
        print("✓ ТЕСТ ПРОЙДЕН: Shared memory обновляется после reread_db")
    else:
        print("✗ ТЕСТ НЕ ПРОЙДЕН: Shared memory не обновилась")
    print("=" * 80)
    
    return test_passed


if __name__ == "__main__":
    success = test_reread_db()
    sys.exit(0 if success else 1)

