#!/usr/bin/env python3
import sys
import time
from pathlib import Path
from lib.utils import load_env_file, run_local_command, SCRIPT_DIR

env_vars = load_env_file()
PROJECT_URL = env_vars.get('PROJECT_URL', 'http://172.26.176.1/boincserver')
ACCOUNT_KEY = env_vars.get('BOINC_ACCOUNT_KEY', '')


def copy_app_config(client_num):
    client_name = f"boinc-client-{client_num}"
    project_dir = "/var/lib/boinc/projects/172.26.176.1_boincserver"
    config_file = f"{project_dir}/app_config.xml"
    
    try:
        result = run_local_command(
            ["docker", "exec", client_name,
                "sh", "-c",
                f"mkdir -p {project_dir} && "
                f"([ -d {config_file} ] && rm -rf {config_file} || true) && "
                f"([ -f {config_file} ] && rm -f {config_file} || true) && "
                f"cp /var/lib/boinc/app_config.xml {config_file} && "
                f"chmod 644 {config_file}"
            ],
            capture_output=True,
            timeout=5,
            check=False
        )
        
        if result.returncode != 0:
            return False
        
        time.sleep(0.5)
        result2 = run_local_command(
            ["docker", "exec", client_name,
                "sh", "-c",
                f"boinccmd --read_cc_config"
            ],
            capture_output=True,
            timeout=5,
            check=False
        )
        
        return result2.returncode == 0
    except:
        return False


def connect_client(client_num, account_key=None):
    client_name = f"boinc-client-{client_num}"
    client_project_url = PROJECT_URL
    
    if account_key is None:
        account_key = ACCOUNT_KEY
    
    if not account_key:
        account_key = ''
    
    try:
        result = run_local_command(
            ["docker", "exec", client_name,
                "sh", "-c",
                f"PASSWD=$(cat /var/lib/boinc/gui_rpc_auth.cfg 2>/dev/null || echo ''); "
                f"boinccmd --project_attach {client_project_url} '{account_key}'"
            ],
            capture_output=True,
            timeout=10,
            check=False
        )
        
        if result.returncode == 0:
            time.sleep(1)
            copy_app_config(client_num)
            return True, None
        else:
            error_msg = result.stderr if isinstance(result.stderr, str) else result.stderr.decode('utf-8', errors='ignore') if result.stderr else ""
            stdout_msg = result.stdout if isinstance(result.stdout, str) else result.stdout.decode('utf-8', errors='ignore') if result.stdout else ""
            return False, error_msg or stdout_msg
    except Exception as e:
        if "Timeout" in str(e):
            return False, "Timeout"
        return False, str(e)


def connect_all_clients(count=20, account_key=None):
    if account_key is None:
        account_key = ACCOUNT_KEY
    
    connected = 0
    failed = 0
    
    for i in range(1, count + 1):
        client_name = f"boinc-client-{i}"
        success, error = connect_client(i, account_key)
        if success:
            connected += 1
        else:
            print(f"✗ {client_name}: {error}", file=sys.stderr)
            failed += 1
    
    return connected > 0


def update_all_clients():
    result = run_local_command("docker ps --format '{{.Names}}' | grep '^boinc-client-'", 
                              shell=True, capture_output=True, check=False)
    
    if result.returncode != 0:
        print("Не удалось получить список клиентов", file=sys.stderr)
        return False
    
    clients = [line.strip() for line in result.stdout.strip().split('\n') if line.strip()]
    
    if not clients:
        return True
    
    project_url = "http://172.26.176.1/boincserver/"
    updated = 0
    failed = 0
    
    for client_name in clients:
        result = run_local_command(f"docker exec {client_name} boinccmd --project {project_url} update 2>&1",
                                  shell=True, capture_output=True, timeout=30, check=False)
        
        if result.returncode == 0:
            updated += 1
        else:
            failed += 1
            error_msg = result.stderr[:50] if result.stderr else ""
            print(f"✗ {client_name}: {error_msg}", file=sys.stderr)
    
    if updated > 0:
        time.sleep(5)
    
    return updated > 0

