#!/usr/bin/env python3
"""
Средняя задача - вычисление средней сложности (около 1-2 секунды)
"""
import time
import sys

def medium_computation():
    """Среднее вычисление"""
    start = time.time()
    
    # Более сложное вычисление: сумма квадратов для большего диапазона
    result = sum(i**2 for i in range(10000))
    
    # Дополнительные вычисления
    total = 0
    for i in range(5000):
        total += i * 2
    
    elapsed = time.time() - start
    
    print("Medium task completed!")
    print("Result: {}".format(result))
    print("Additional computation: {}".format(total))
    print("Time: {:.3f} seconds".format(elapsed))
    
    # Сохранить результат
    with open('/root/shared/results/result.txt', 'w') as f:
        f.write("Medium task result: {}\n".format(result))
        f.write("Additional computation: {}\n".format(total))
        f.write("Computation time: {:.3f} seconds\n".format(elapsed))

if __name__ == "__main__":
    medium_computation()


