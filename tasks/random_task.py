#!/usr/bin/env python3
"""
Задача с рандомной сложностью - случайное время выполнения
"""
import time
import random
import sys

def random_computation():
    """Вычисление со случайной сложностью"""
    start = time.time()
    
    # Случайная сложность: от 0.5 до 4.5 секунд
    target_time = random.uniform(0.5, 4.5)
    
    result = 0
    iterations = 0
    
    while time.time() - start < target_time:
        # Вычисляем сумму квадратов
        for i in range(random.randint(1000, 10000)):
            result += i**2
        iterations += 1
    
    elapsed = time.time() - start
    
    print("Random task completed!")
    print("Target time: {:.3f} seconds".format(target_time))
    print("Actual time: {:.3f} seconds".format(elapsed))
    print("Result: {}".format(result))
    print("Iterations: {}".format(iterations))
    
    # Сохранить результат
    with open('/root/shared/results/result.txt', 'w') as f:
        f.write("Random task result: {}\n".format(result))
        f.write("Target time: {:.3f} seconds\n".format(target_time))
        f.write("Actual time: {:.3f} seconds\n".format(elapsed))
        f.write("Iterations: {}\n".format(iterations))

if __name__ == "__main__":
    random_computation()


