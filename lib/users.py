#!/usr/bin/env python3
import hashlib
import urllib.parse
import xml.etree.ElementTree as ET
import sys
from pathlib import Path
from lib.utils import load_env_file, run_local_command, SCRIPT_DIR

def md5_hash(text):
    return hashlib.md5(text.encode('utf-8')).hexdigest()


def _set_max_jobs_for_user(email):
    email_escaped = email.replace("'", "''")
    sql = f"""
    UPDATE user
    SET project_prefs = CASE
        WHEN project_prefs IS NULL OR project_prefs = '' THEN
            '<max_jobs_in_progress>1</max_jobs_in_progress>'
        WHEN project_prefs NOT LIKE '%<max_jobs_in_progress>%' THEN
            CONCAT(project_prefs, '<max_jobs_in_progress>1</max_jobs_in_progress>')
        ELSE
            REGEXP_REPLACE(project_prefs, '<max_jobs_in_progress>[0-9]+</max_jobs_in_progress>', '<max_jobs_in_progress>1</max_jobs_in_progress>')
    END
    WHERE email_addr = '{email_escaped}';
    """
    
    try:
        result = run_local_command(
            ["docker", "exec", "-i", "server-apache-1",
                "bash", "-c", "cd /home/boincadm/project && mysql -u root -ppassword boincserver"],
            input=sql,
            check=False
        )
    except Exception:
        pass


def create_user(email, password, user_name, project_url):
    passwd_hash = md5_hash(password)
    
    try:
        params = {
            'email_addr': email.lower(),
            'passwd_hash': passwd_hash,
            'user_name': user_name
        }
        query_string = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
        url = f"http://localhost/boincserver/create_account.php?{query_string}"
        
        result = run_local_command(["docker", "exec", "server-apache-1", "curl", "-s", url],
                                  capture_output=True, timeout=30, check=False)
        
        if result.returncode != 0:
            return False, None, f"Ошибка выполнения команды: {result.stderr}"
        
        response_text = result.stdout
        
        if not response_text or len(response_text.strip()) == 0:
            return False, None, f"Пустой ответ от сервера. stderr: {result.stderr}"
        
        try:
            root = ET.fromstring(response_text)
        except ET.ParseError as e:
            return False, None, f"Ошибка парсинга XML. Ответ: {response_text[:500]}"
        
        error = root.find('error')
        if error is not None:
            error_num_elem = error.find('error_num')
            error_msg_elem = error.find('error_msg')
            error_num = error_num_elem.text if error_num_elem is not None else 'unknown'
            error_msg = error_msg_elem.text if error_msg_elem is not None else 'Unknown error'
            return False, None, f"Ошибка {error_num}: {error_msg}"
        
        account_out = root.find('account_out')
        if account_out is None:
            authenticator = root.find('.//authenticator')
        else:
            authenticator = account_out.find('authenticator')
            if authenticator is None:
                authenticator = account_out.find('.//authenticator')
        
        if authenticator is not None:
            account_key = authenticator.text
            if account_key:
                account_key = account_key.strip()
                if account_key:
                    _set_max_jobs_for_user(email.lower())
                    return True, account_key, "Пользователь успешно создан"
        
        return False, None, f"Не удалось получить account key из ответа. XML: {response_text[:500]}"
        
    except subprocess.TimeoutExpired:
        return False, None, "Таймаут выполнения запроса"
    except ET.ParseError as e:
        return False, None, f"Ошибка парсинга XML: {e}"
    except Exception as e:
        return False, None, f"Неожиданная ошибка: {e}"


def lookup_account(email, password, project_url):
    passwd_hash = md5_hash(password)
    
    try:
        params = {
            'email_addr': email.lower(),
            'passwd_hash': passwd_hash
        }
        query_string = urllib.parse.urlencode(params)
        url = f"http://localhost/boincserver/lookup_account.php?{query_string}"
        
        result = run_local_command(["docker", "exec", "server-apache-1", "curl", "-s", url],
                                  capture_output=True, timeout=30, check=False)
        
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
    env_path = SCRIPT_DIR / '.env'
    if not env_path.exists():
        env_path.touch()
    
    lines = []
    if env_path.exists():
        with open(env_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    
    found = False
    for i, line in enumerate(lines):
        if line.startswith('BOINC_ACCOUNT_KEY='):
            lines[i] = f'BOINC_ACCOUNT_KEY={account_key}\n'
            found = True
            break
    
    if not found:
        lines.append(f'BOINC_ACCOUNT_KEY={account_key}\n')
    
    with open(env_path, 'w', encoding='utf-8') as f:
        f.writelines(lines)


def create_or_lookup_user():
    env_vars = load_env_file()
    project_url = env_vars.get('PROJECT_URL', 'http://172.26.176.1/boincserver')
    email = env_vars.get('BOINC_USER_EMAIL', 'test@test.com')
    password = env_vars.get('BOINC_USER_PASSWORD', 'testpass')
    user_name = env_vars.get('BOINC_USER_NAME', 'TestUser')
    
    success, account_key, message = lookup_account(email, password, project_url)
    
    if success:
        update_env_account_key(account_key)
        return True, account_key
    
    success, account_key, message = create_user(email, password, user_name, project_url)
    
    if success:
        update_env_account_key(account_key)
        return True, account_key
    else:
        print(f"✗ {message}", file=sys.stderr)
        return False, None

