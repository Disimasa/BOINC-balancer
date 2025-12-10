#!/usr/bin/env python3
"""
Быстрая задача - простое вычисление (около 0.1-0.5 секунды)
"""
import time
import sys

def fast_computation():
    """Быстрое вычисление"""
    start = time.time()
    
    # Простое вычисление: сумма квадратов
    result = sum(i**2 for i in range(1000))
    
    elapsed = time.time() - start
    
    print("Fast task completed!")
    print("Result: {}".format(result))
    print("Time: {:.3f} seconds".format(elapsed))
    
    # Сохранить результат
    with open('/root/shared/results/result.txt', 'w') as f:
        f.write("Fast task result: {}\n".format(result))
        f.write("Computation time: {:.3f} seconds\n".format(elapsed))

if __name__ == "__main__":
    fast_computation()


