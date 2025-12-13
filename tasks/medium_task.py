#!/usr/bin/env python3
"""
Средняя задача - точное время выполнения: 3 секунды
"""
import time
import sys
import os

def medium_computation():
    """Средняя задача с точным временем выполнения: ~3 секунды"""
    start = time.time()
    
    # Вычисления для нагружения CPU (~3 секунды)
    # Вычисление факториала и суммы
    result = 0
    iterations = 60000000  # Примерно 3 секунды на среднем CPU
    for i in range(1, iterations + 1):
        result += i * (i + 1) * (i + 2)
    
    elapsed = time.time() - start
    
    print("Medium task completed!")
    print("Time: {:.3f} seconds".format(elapsed))
    
    # Сохранить результат
    # BOINC ожидает файл с логическим именем (open_name) в текущей директории
    with open('result.txt', 'w') as f:
        f.write("Medium task completed\n")
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
    medium_computation()


