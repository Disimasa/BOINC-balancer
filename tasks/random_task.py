#!/usr/bin/env python3
"""
Задача с рандомной сложностью - случайное время выполнения от 1 до 6 секунд
"""
import time
import random
import sys
import os

def random_computation():
    """Задача со случайным временем выполнения: от ~1 до ~6 секунд"""
    start = time.time()
    
    # Случайное количество итераций для разного времени выполнения
    # От ~1 до ~6 секунд на среднем CPU
    min_iterations = 20000000   # ~1 секунда
    max_iterations = 120000000  # ~6 секунд
    iterations = random.randint(min_iterations, max_iterations)
    
    # Вычисления для нагружения CPU
    result = 0
    for i in range(iterations):
        # Смешанные вычисления
        result += i * (i % 100) + (i * i) % 1000
    
    elapsed = time.time() - start
    
    print("Random task completed!")
    print("Iterations: {}".format(iterations))
    print("Actual time: {:.3f} seconds".format(elapsed))
    
    # Сохранить результат
    # BOINC ожидает файл с логическим именем (open_name) в текущей директории
    with open('result.txt', 'w') as f:
        f.write("Random task completed\n")
        f.write("Iterations: {}\n".format(iterations))
        f.write("Execution time: {:.3f} seconds\n".format(elapsed))
        f.write("Result: {}\n".format(result % 1000000))  # Последние 6 цифр для проверки
        f.flush()
        os.fsync(f.fileno())
    
    # Создаем файл boinc_finish_called для уведомления BOINC об успешном завершении
    with open('boinc_finish_called', 'w') as f:
        f.write('0\n')
        f.flush()
        os.fsync(f.fileno())

if __name__ == "__main__":
    random_computation()


