#!/usr/bin/env python3
"""
Быстрая задача - время выполнения: 1 секунда
"""
import time
import sys
import os

def fast_computation():
    """Быстрая задача с временем выполнения: 1 секунда"""
    start = time.time()
    
    # Простой sleep на 1 секунду
    time.sleep(1)
    
    elapsed = time.time() - start
    
    print("Fast task completed!")
    print("Time: {:.3f} seconds".format(elapsed))
    
    # Сохранить результат
    cwd = os.getcwd()
    output_file = 'result.txt'
    output_path = os.path.join(cwd, output_file)
    
    print("Current directory: {}".format(cwd), file=sys.stderr)
    print("Writing output to: {}".format(output_path), file=sys.stderr)
    
    with open(output_file, 'w') as f:
        f.write("Fast task completed\n")
        f.write("Execution time: {:.3f} seconds\n".format(elapsed))
        f.flush()
        os.fsync(f.fileno())
    
    # Проверяем, что файл создан
    if os.path.exists(output_file):
        size = os.path.getsize(output_file)
        print("Output file created: {} ({} bytes)".format(output_path, size), file=sys.stderr)
    else:
        print("ERROR: Output file not created: {}".format(output_path), file=sys.stderr)
        sys.exit(1)
    
    # Создаем файл boinc_finish_called для уведомления BOINC об успешном завершении
    finish_file = 'boinc_finish_called'
    with open(finish_file, 'w') as f:
        f.write('0\n')
        f.flush()
        os.fsync(f.fileno())
    print("Finish file created: {}".format(finish_file), file=sys.stderr)

if __name__ == "__main__":
    fast_computation()
