#!/usr/bin/env python3
"""
Долгая задача - время выполнения: 30 секунд
"""
import time
import sys
import os

def long_computation():
    """Долгая задача с временем выполнения: 30 секунд"""
    start = time.time()
    
    # Простой sleep на 30 секунд
    time.sleep(30)
    
    elapsed = time.time() - start
    
    print("Long task completed!")
    print("Time: {:.3f} seconds".format(elapsed))
    
    # Сохранить результат
    with open('result.txt', 'w') as f:
        f.write("Long task completed\n")
        f.write("Execution time: {:.3f} seconds\n".format(elapsed))
        f.flush()
        os.fsync(f.fileno())
    
    # Создаем файл boinc_finish_called для уведомления BOINC об успешном завершении
    with open('boinc_finish_called', 'w') as f:
        f.write('0\n')
        f.flush()
        os.fsync(f.fileno())

if __name__ == "__main__":
    long_computation()
