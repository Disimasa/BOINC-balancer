#!/usr/bin/env python3
"""
Скрипт для создания пользователя в BOINC проекте
"""
import os
import sys
import hashlib
import subprocess
import urllib.parse
import xml.etree.ElementTree as ET
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
BOINC_USER_EMAIL = env_vars.get('BOINC_USER_EMAIL', 'test@test.com')
BOINC_USER_PASSWORD = env_vars.get('BOINC_USER_PASSWORD', 'testpass')
BOINC_USER_NAME = env_vars.get('BOINC_USER_NAME', 'TestUser')


def md5_hash(text):
    """Вычисляет MD5 хеш строки"""
    return hashlib.md5(text.encode('utf-8')).hexdigest()


def create_user(email, password, user_name, project_url):
    """
    Создает пользователя в BOINC проекте через API
    
    Returns:
        tuple: (success: bool, account_key: str, message: str)
    """
    import subprocess
    
    # BOINC требует MD5 хеш пароля (32 символа)
    passwd_hash = md5_hash(password)
    
    # Используем docker exec для выполнения запроса из контейнера apache
    # где сервер точно доступен
    try:
        print(f"Отправка запроса на создание аккаунта...")
        print(f"  URL: {project_url}/create_account.php")
        print(f"  Email: {email}")
        print(f"  User name: {user_name}")
        
        # BOINC get_str() проверяет только $_GET, поэтому используем GET параметры
        # URL-кодируем параметры (urlencode правильно кодирует специальные символы)
        params = {
            'email_addr': email.lower(),
            'passwd_hash': passwd_hash,
            'user_name': user_name
        }
        query_string = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
        url = f"http://localhost/boincserver/create_account.php?{query_string}"
        
        print(f"  Сгенерированный URL: {url[:100]}...")
        
        # Выполняем curl через docker exec в контейнере apache
        cmd = [
            "wsl.exe", "-e", "docker", "exec", "server-apache-1",
            "curl", "-s", url
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode != 0:
            return False, None, f"Ошибка выполнения команды: {result.stderr}"
        
        response_text = result.stdout
        
        # Отладочный вывод
        if not response_text or len(response_text.strip()) == 0:
            return False, None, f"Пустой ответ от сервера. stderr: {result.stderr}"
        
        print(f"  Ответ сервера: {response_text[:200]}...")
        
        # Парсим XML ответ
        try:
            root = ET.fromstring(response_text)
        except ET.ParseError as e:
            return False, None, f"Ошибка парсинга XML. Ответ: {response_text[:500]}"
        
        # Проверяем наличие ошибок
        error = root.find('error')
        if error is not None:
            error_num_elem = error.find('error_num')
            error_msg_elem = error.find('error_msg')
            error_num = error_num_elem.text if error_num_elem is not None else 'unknown'
            error_msg = error_msg_elem.text if error_msg_elem is not None else 'Unknown error'
            return False, None, f"Ошибка {error_num}: {error_msg}"
        
        # Получаем authenticator (account key)
        # Пробуем разные способы поиска
        account_out = root.find('account_out')
        if account_out is None:
            # Пробуем найти напрямую
            authenticator = root.find('.//authenticator')
        else:
            authenticator = account_out.find('authenticator')
            if authenticator is None:
                # Пробуем найти в любом месте внутри account_out
                authenticator = account_out.find('.//authenticator')
        
        if authenticator is not None:
            account_key = authenticator.text
            if account_key:
                account_key = account_key.strip()
                if account_key:
                    return True, account_key, "Пользователь успешно создан"
        
        return False, None, f"Не удалось получить account key из ответа. XML: {response_text[:500]}"
        
    except subprocess.TimeoutExpired:
        return False, None, "Таймаут выполнения запроса"
    except ET.ParseError as e:
        return False, None, f"Ошибка парсинга XML: {e}"
    except Exception as e:
        return False, None, f"Неожиданная ошибка: {e}"


def lookup_account(email, password, project_url):
    """
    Получает account key для существующего пользователя через lookup_account
    
    Returns:
        tuple: (success: bool, account_key: str, message: str)
    """
    import subprocess
    
    passwd_hash = md5_hash(password)
    
    try:
        print(f"Попытка получить account key для существующего пользователя...")
        
        # BOINC get_str() проверяет только $_GET, поэтому используем GET параметры
        params = {
            'email_addr': email.lower(),
            'passwd_hash': passwd_hash
        }
        query_string = urllib.parse.urlencode(params)
        url = f"http://localhost/boincserver/lookup_account.php?{query_string}"
        
        # Выполняем curl через docker exec в контейнере apache
        cmd = [
            "wsl.exe", "-e", "docker", "exec", "server-apache-1",
            "curl", "-s", url
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode != 0:
            return False, None, f"Ошибка выполнения команды: {result.stderr}"
        
        response_text = result.stdout
        root = ET.fromstring(response_text)
        
        error = root.find('error')
        if error is not None:
            error_num = error.get('num', 'unknown')
            return False, None, f"Ошибка {error_num}: Пользователь не найден или неверный пароль"
        
        account_out = root.find('account_out')
        if account_out is not None:
            authenticator = account_out.find('authenticator')
            if authenticator is not None and authenticator.text:
                account_key = authenticator.text.strip()
                return True, account_key, "Account key получен"
        
        return False, None, "Не удалось получить account key"
        
    except subprocess.TimeoutExpired:
        return False, None, "Таймаут выполнения запроса"
    except Exception as e:
        return False, None, f"Ошибка: {e}"


def update_env_account_key(account_key):
    """Обновляет BOINC_ACCOUNT_KEY в .env файле"""
    if not env_path.exists():
        print(f"Внимание: файл {env_path} не существует, создаю новый...")
        env_path.touch()
    
    # Читаем содержимое .env файла
    lines = []
    if env_path.exists():
        with open(env_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    
    # Обновляем или добавляем BOINC_ACCOUNT_KEY
    found = False
    for i, line in enumerate(lines):
        if line.startswith('BOINC_ACCOUNT_KEY='):
            lines[i] = f'BOINC_ACCOUNT_KEY={account_key}\n'
            found = True
            break
    
    if not found:
        lines.append(f'BOINC_ACCOUNT_KEY={account_key}\n')
    
    # Записываем обновленное содержимое обратно в .env
    with open(env_path, 'w', encoding='utf-8') as f:
        f.writelines(lines)
    print(f"✓ Account key сохранен в {env_path}")


def main():
    print("=== Создание пользователя в BOINC проекте ===\n")
    print(f"URL проекта: {PROJECT_URL}")
    print(f"Email: {BOINC_USER_EMAIL}")
    print(f"User name: {BOINC_USER_NAME}\n")
    
    # Сначала пытаемся получить account key для существующего пользователя
    success, account_key, message = lookup_account(
        BOINC_USER_EMAIL, 
        BOINC_USER_PASSWORD, 
        PROJECT_URL
    )
    
    if success:
        print(f"✓ {message}")
        print(f"\nAccount key: {account_key}\n")
        update_env_account_key(account_key)
        print("Пользователь уже существует. Используйте этот account key для подключения клиентов.")
        return 0
    
    # Если пользователь не найден, создаем нового
    print(f"Пользователь не найден. Создание нового аккаунта...\n")
    success, account_key, message = create_user(
        BOINC_USER_EMAIL,
        BOINC_USER_PASSWORD,
        BOINC_USER_NAME,
        PROJECT_URL
    )
    
    if success:
        print(f"✓ {message}")
        print(f"\nAccount key: {account_key}\n")
        update_env_account_key(account_key)
        print("Пользователь успешно создан! Используйте этот account key для подключения клиентов.")
        return 0
    else:
        print(f"✗ {message}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

