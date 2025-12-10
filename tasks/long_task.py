#!/usr/bin/env python3
"""
Долгая задача - вычисление максимальной сложности (до 5 секунд)
"""
import time
import sys

def long_computation():
    """Долгое вычисление (до 5 секунд)"""
    start = time.time()
    max_time = 5.0  # Максимум 5 секунд
    
    # Сложное вычисление: большие вычисления
    result = 0
    iterations = 0
    
    while time.time() - start < max_time:
        # Вычисляем сумму квадратов для большого диапазона
        for i in range(10000):
            result += i**2
        iterations += 1
    
    elapsed = time.time() - start
    
    print("Long task completed!")
    print("Result: {}".format(result))
    print("Iterations: {}".format(iterations))
    print("Time: {:.3f} seconds".format(elapsed))
    
    # Сохранить результат
    with open('/root/shared/results/result.txt', 'w') as f:
        f.write("Long task result: {}\n".format(result))
        f.write("Iterations: {}\n".format(iterations))
        f.write("Computation time: {:.3f} seconds\n".format(elapsed))

if __name__ == "__main__":
    long_computation()


