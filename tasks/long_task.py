#!/usr/bin/env python3
"""
Долгая задача - точное время выполнения: 6 секунд
"""
import time
import sys
import os

def long_computation():
    """Долгая задача с точным временем выполнения: ~6 секунд"""
    start = time.time()
    
    # Вычисления для нагружения CPU (~6 секунд)
    # Более сложные вычисления: поиск простых чисел и вычисления
    result = 0
    iterations = 120000000  # Примерно 6 секунд на среднем CPU
    for i in range(1, iterations + 1):
        # Вычисление суммы кубов с дополнительными операциями
        result += i * i * i + (i % 1000) * (i % 500)
    
    elapsed = time.time() - start
    
    print("Long task completed!")
    print("Time: {:.3f} seconds".format(elapsed))
    
    # Сохранить результат
    # BOINC ожидает файл с логическим именем (open_name) в текущей директории
    with open('result.txt', 'w') as f:
        f.write("Long task completed\n")
        f.write("Execution time: {:.3f} seconds\n".format(elapsed))
        f.write("Iterations: {}\n".format(iterations))
        f.write("Result: {}\n".format(result % 1000000))  # Последние 6 цифр для проверки
        f.flush()
        os.fsync(f.fileno())
    
    # Создаем файл boinc_finish_called для уведомления BOINC об успешном завершении
    with open('boinc_finish_called', 'w') as f:
        f.write('0\n')
        f.flush()
        os.fsync(f.fileno())

if __name__ == "__main__":
    long_computation()


