#!/usr/bin/env python3
"""
Задача с рандомной сложностью - случайное время выполнения от 1 до 6 секунд
"""
import time
import random
import sys
import os

def random_computation():
    """Задача со случайным временем выполнения: от 1 до 6 секунд"""
    start = time.time()
    
    # Случайное время выполнения: от 1 до 6 секунд
    target_time = random.uniform(1.0, 6.0)
    
    # Точное время выполнения через sleep
    time.sleep(target_time)
    
    elapsed = time.time() - start
    
    print("Random task completed!")
    print("Target time: {:.3f} seconds".format(target_time))
    print("Actual time: {:.3f} seconds".format(elapsed))
    
    # Сохранить результат
    # BOINC ожидает файл с логическим именем (open_name) в текущей директории
    with open('result.txt', 'w') as f:
        f.write("Random task completed\n")
        f.write("Target time: {:.3f} seconds\n".format(target_time))
        f.write("Actual time: {:.3f} seconds\n".format(elapsed))
        f.flush()
        os.fsync(f.fileno())
    
    # Создаем файл boinc_finish_called для уведомления BOINC об успешном завершении
    with open('boinc_finish_called', 'w') as f:
        f.write('0\n')
        f.flush()
        os.fsync(f.fileno())

if __name__ == "__main__":
    random_computation()


