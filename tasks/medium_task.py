#!/usr/bin/env python3
"""
Средняя задача - время выполнения: 10 секунд
"""
import time
import sys
import os

def medium_computation():
    """Средняя задача с временем выполнения: 10 секунд"""
    start = time.time()
    
    # Простой sleep на 10 секунд
    time.sleep(10)
    
    elapsed = time.time() - start
    
    print("Medium task completed!")
    print("Time: {:.3f} seconds".format(elapsed))
    
    # Сохранить результат
    with open('result.txt', 'w') as f:
        f.write("Medium task completed\n")
        f.write("Execution time: {:.3f} seconds\n".format(elapsed))
        f.flush()
        os.fsync(f.fileno())
    
    # Создаем файл boinc_finish_called для уведомления BOINC об успешном завершении
    with open('boinc_finish_called', 'w') as f:
        f.write('0\n')
        f.flush()
        os.fsync(f.fileno())

if __name__ == "__main__":
    medium_computation()
