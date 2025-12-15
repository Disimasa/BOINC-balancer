#!/usr/bin/env python3
import os
import sys
import time
from lib.utils import run_command, PROJECT_HOME, CONTAINER_NAME

PROJECT_ROOT = os.environ.get("PROJECT_ROOT", "/home/boincadm/project")
PROJECT_NAME = os.environ.get("PROJECT", "boincserver")


def wait_for_project():
    for i in range(60):
        cmd = f"test -f {PROJECT_ROOT}/.built_{PROJECT_NAME} && echo ready"
        stdout, success = run_command(f"cd {PROJECT_ROOT} && {cmd}", check=False, capture_output=True)
        if success and "ready" in stdout:
            return True
        time.sleep(2)
    
    print("⚠ Предупреждение: не удалось дождаться создания проекта, продолжаем...", file=sys.stderr)
    return False


def check_key_exists():
    cmd = f"""cd {PROJECT_ROOT} && if [ -f keys/code_sign_private ] && [ ! -c keys/code_sign_private ] && [ -s keys/code_sign_private ]; then echo 'EXISTS'; ls -lh keys/code_sign_private keys/code_sign_public 2>/dev/null || true; else echo 'NEED_CREATE'; fi"""
    stdout, success = run_command(cmd, check=False, capture_output=True)
    return stdout, success


def ensure_keys_directory():
    check_symlink_cmd = f"""cd {PROJECT_ROOT} && if [ -L keys ]; then echo 'SYMLINK'; readlink -f keys; elif [ -d keys ]; then echo 'DIRECTORY'; else echo 'NOT_EXISTS'; fi"""
    symlink_output, _ = run_command(check_symlink_cmd, check=False, capture_output=True)
    
    if "SYMLINK" in symlink_output:
        target_dir = symlink_output.split('\n')[1] if '\n' in symlink_output else ""
        
        remove_symlink_cmd = f"""cd {PROJECT_ROOT} && rm -f keys && mkdir -p keys && chown boincadm:boincadm keys"""
        run_command(remove_symlink_cmd, check=False, capture_output=True)
        
        if target_dir:
            copy_keys_cmd = f"""if [ -f {target_dir}/code_sign_public ] && [ ! -c {target_dir}/code_sign_public ]; then cp {target_dir}/code_sign_public {PROJECT_ROOT}/keys/ 2>/dev/null || true; fi && if [ -f {target_dir}/upload_private ] && [ ! -c {target_dir}/upload_private ]; then cp {target_dir}/upload_private {PROJECT_ROOT}/keys/ 2>/dev/null || true; fi && if [ -f {target_dir}/upload_public ] && [ ! -c {target_dir}/upload_public ]; then cp {target_dir}/upload_public {PROJECT_ROOT}/keys/ 2>/dev/null || true; fi && chown boincadm:boincadm {PROJECT_ROOT}/keys/* 2>/dev/null || true"""
            run_command(copy_keys_cmd, check=False, capture_output=True)
    else:
        cmd = f"""cd {PROJECT_ROOT} && mkdir -p keys && chown boincadm:boincadm keys 2>/dev/null || true && echo 'Directory ready'"""
        run_command(cmd, check=False, capture_output=True)
    
    return True


def generate_keys():
    if not ensure_keys_directory():
        print("✗ Ошибка: не удалось подготовить директорию keys", file=sys.stderr)
        return False
    
    keys_dir = f"{PROJECT_ROOT}/keys"
    
    remove_old_keys_cmd = f"""cd {PROJECT_ROOT} && if [ -c keys/code_sign_private ]; then rm -f keys/code_sign_private 2>&1 || unlink keys/code_sign_private 2>&1 || true; fi; rm -f keys/code_sign_private keys/code_sign_public 2>&1 || true;"""
    run_command(remove_old_keys_cmd, check=False, capture_output=True)
    
    cmd = f"""cd {PROJECT_ROOT} && bin/crypt_prog -genkey 1024 keys/code_sign_private keys/code_sign_public 2>&1 && sync && chown boincadm:boincadm keys/code_sign_private keys/code_sign_public 2>/dev/null || true && echo 'Keys generated'"""
    
    _, success = run_command(cmd, check=False, capture_output=True)
    if not success:
        print("✗ Ошибка при генерации ключей", file=sys.stderr)
        return False
    
    time.sleep(1)
    return True


def verify_keys():
    cmd = f"""cd {PROJECT_ROOT} && if [ -f keys/code_sign_private ] && [ -s keys/code_sign_private ] && [ -f keys/code_sign_public ] && [ -s keys/code_sign_public ]; then echo 'SUCCESS'; echo 'Private key:'; ls -lh keys/code_sign_private; echo 'Public key:'; ls -lh keys/code_sign_public; echo 'Key content (first 100 chars):'; head -c 100 keys/code_sign_private; echo ''; else echo 'FAILED'; ls -la keys/ 2>&1 || true; fi"""
    stdout, success = run_command(cmd, check=False, capture_output=True)
    return stdout, success


def generate_signing_keys():
    wait_for_project()
    
    check_output, check_success = check_key_exists()
    
    if "EXISTS" in check_output:
        return True
    
    if "NEED_CREATE" not in check_output:
        print(f"⚠ Неожиданный результат проверки: {check_output}", file=sys.stderr)
    
    if not generate_keys():
        return False
    
    verify_output, verify_success = verify_keys()
    
    if "SUCCESS" in verify_output:
        return True
    else:
        print("✗ Ключи не были созданы или пустые", file=sys.stderr)
        print(verify_output, file=sys.stderr)
        return False

