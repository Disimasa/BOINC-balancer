#!/usr/bin/env python3
"""
Задача с рандомной сложностью - случайное время выполнения от 5 до 15 секунд
"""
import time
import random
import sys
import os

def random_computation():
    """Задача со случайным временем выполнения: от 5 до 15 секунд"""
    start = time.time()
    
    # Случайное время от 5 до 15 секунд
    sleep_time = random.uniform(5, 15)
    
    # Простой sleep на случайное время
    time.sleep(sleep_time)
    
    elapsed = time.time() - start
    
    print("Random task completed!")
    print("Sleep time: {:.3f} seconds".format(sleep_time))
    print("Actual time: {:.3f} seconds".format(elapsed))
    
    # Сохранить результат
    with open('result.txt', 'w') as f:
        f.write("Random task completed\n")
        f.write("Sleep time: {:.3f} seconds\n".format(sleep_time))
        f.write("Execution time: {:.3f} seconds\n".format(elapsed))
        f.flush()
        os.fsync(f.fileno())
    
    # Создаем файл boinc_finish_called для уведомления BOINC об успешном завершении
    with open('boinc_finish_called', 'w') as f:
        f.write('0\n')
        f.flush()
        os.fsync(f.fileno())

if __name__ == "__main__":
    random_computation()
