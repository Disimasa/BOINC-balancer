#!/usr/bin/env python3
"""
Скрипт для подключения клиентов BOINC к серверу
"""
import subprocess
import sys
import time
from pathlib import Path

# Загружаем переменные окружения из .env вручную
def load_env_file(env_path):
    """Загружает переменные из .env файла"""
    env_vars = {}
    if env_path.exists():
        with open(env_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                # Пропускаем комментарии и пустые строки
                if not line or line.startswith('#'):
                    continue
                # Парсим KEY=VALUE
                if '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip().strip('"').strip("'")
                    env_vars[key] = value
    return env_vars

env_path = Path(__file__).parent / '.env'
env_vars = load_env_file(env_path)

PROJECT_URL = env_vars.get('PROJECT_URL', 'http://172.26.176.1/boincserver')
DEFAULT_EMAIL = env_vars.get('BOINC_USER_EMAIL', 'test@test.com')
DEFAULT_PASSWORD = env_vars.get('BOINC_USER_PASSWORD', 'testpass')
ACCOUNT_KEY = env_vars.get('BOINC_ACCOUNT_KEY', '')

def connect_client(client_num, account_key=None):
    """Подключить клиент к проекту"""
    client_name = f"boinc-client-{client_num}"
    client_project_url = PROJECT_URL
    
    # Используем account key из .env, если не передан явно
    if account_key is None:
        account_key = ACCOUNT_KEY
    
    # Если account key не указан, используем пустую строку
    # (BOINC попытается зарегистрировать пользователя)
    if not account_key:
        account_key = ''
    
    try:
        result = subprocess.run(
            [
                "wsl.exe", "-e", "docker", "exec", client_name,
                "sh", "-c",
                f"PASSWD=$(cat /var/lib/boinc/gui_rpc_auth.cfg 2>/dev/null || echo ''); "
                f"boinccmd --passwd \"$PASSWD\" --project_attach {client_project_url} '{account_key}'"
            ],
            capture_output=True,
            timeout=10
        )
        
        if result.returncode == 0:
            return True, None
        else:
            error_msg = result.stderr.decode('utf-8', errors='ignore')
            stdout_msg = result.stdout.decode('utf-8', errors='ignore')
            return False, error_msg or stdout_msg
    except subprocess.TimeoutExpired:
        return False, "Timeout"
    except Exception as e:
        return False, str(e)

def get_client_state(client_num):
    """Получить статус клиента"""
    client_name = f"boinc-client-{client_num}"
    
    try:
        result = subprocess.run(
            ["wsl.exe", "-e", "docker", "exec", client_name, "boinccmd", "--get_state"],
            capture_output=True,
            timeout=5
        )
        
        if result.returncode == 0:
            output = result.stdout.decode('utf-8', errors='ignore')
            return output.strip()
        return None
    except:
        return None

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Подключение клиентов к BOINC проекту')
    parser.add_argument('--account-key', default=None, help='Account key для подключения (по умолчанию из .env)')
    parser.add_argument('--count', type=int, default=20, help='Количество клиентов для подключения')
    
    args = parser.parse_args()
    
    print("=== Подключение клиентов к проекту ===\n")
    print(f"URL проекта: {PROJECT_URL}")
    if ACCOUNT_KEY:
        print(f"Account key: {ACCOUNT_KEY[:20]}... (из .env)")
    else:
        print("Account key: не указан (будет использована пустая строка для регистрации)")
    print(f"Количество клиентов: {args.count}\n")
    
    # Используем account key из аргументов или из .env
    account_key = args.account_key or ACCOUNT_KEY
    
    print("Подключение клиентов...\n")
    success_count = 0
    failed_count = 0
    
    for i in range(1, args.count + 1):
        print(f"Подключение boinc-client-{i}... ", end="", flush=True)
        
        success, error = connect_client(i, account_key)
        
        if success:
            print("✓ Успешно")
            success_count += 1
        else:
            if "already attached" in (error or "").lower() or "already" in (error or "").lower():
                print("✓ Уже подключен")
                success_count += 1
            else:
                print(f"✗ Ошибка: {error or 'Неизвестная ошибка'}")
                failed_count += 1
        
        time.sleep(0.5)
    
    print(f"\n=== Результаты ===")
    print(f"Успешно подключено: {success_count}")
    print(f"Ошибок: {failed_count}\n")
    
    print("Проверка статуса клиентов (первые 5):")
    for i in range(1, min(6, args.count + 1)):
        state = get_client_state(i)
        if state:
            projects = [line for line in state.split('\n') if 'Projects' in line or 'project' in line.lower()]
            print(f"  boinc-client-{i}: {len(projects)} проектов")
        else:
            print(f"  boinc-client-{i}: Не удалось получить статус")
    
    print("\n✓ Готово!")

if __name__ == "__main__":
    main()

