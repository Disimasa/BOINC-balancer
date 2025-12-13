#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для генерации ключей подписи кода BOINC.
Проверяет наличие ключа и создает его, если он отсутствует или пустой.
"""

from __future__ import print_function
import subprocess
import sys
import time
import os
from pathlib import Path

# Определяем директорию скрипта
SCRIPT_DIR = Path(__file__).parent.absolute()

# Константы
PROJECT_ROOT = os.environ.get("PROJECT_ROOT", "/home/boincadm/project")
CONTAINER_NAME = "server-apache-1"
PROJECT_NAME = os.environ.get("PROJECT", "boincserver")


def run_command(cmd, check=True, capture_output=False):
    """Выполнить команду в контейнере"""
    if isinstance(cmd, str):
        full_cmd = ["wsl.exe", "-e", "docker", "exec", CONTAINER_NAME, "bash", "-c", cmd]
    else:
        full_cmd = ["wsl.exe", "-e", "docker", "exec", CONTAINER_NAME] + cmd
    
    try:
        if capture_output:
            result = subprocess.run(
                full_cmd,
                capture_output=True,
                text=True,
                timeout=60
            )
            return result.stdout.strip(), result.returncode == 0
        else:
            proc = subprocess.Popen(
                full_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1
            )
            for line in proc.stdout:
                print(line, end='', flush=True)
            proc.wait()
            if check and proc.returncode != 0:
                print(f"\n✗ Ошибка: команда завершилась с кодом {proc.returncode}", file=sys.stderr)
                return False
            return True
    except subprocess.TimeoutExpired:
        print("✗ Ошибка: команда превысила время ожидания", file=sys.stderr)
        return False
    except Exception as e:
        print(f"✗ Исключение при выполнении команды: {e}", file=sys.stderr)
        return False


def wait_for_project():
    """Дождаться создания проекта"""
    print("Ожидание создания проекта...")
    for i in range(60):
        cmd = f"test -f {PROJECT_ROOT}/.built_{PROJECT_NAME} && echo ready"
        stdout, success = run_command(cmd, check=False, capture_output=True)
        if success and "ready" in stdout:
            print("✓ Проект создан")
            return True
        if i < 59:  # Не выводим сообщение на последней итерации
            print(f"  Попытка {i+1}/60...")
        time.sleep(2)
    
    print("⚠ Предупреждение: не удалось дождаться создания проекта, продолжаем...", file=sys.stderr)
    return False


def check_key_exists():
    """Проверить, существует ли ключ и не пустой ли он"""
    cmd = (
        f"cd {PROJECT_ROOT} && "
        f"if [ -f keys/code_sign_private ] && [ ! -c keys/code_sign_private ] && [ -s keys/code_sign_private ]; then "
        f"  echo 'EXISTS'; "
        f"  ls -lh keys/code_sign_private keys/code_sign_public 2>/dev/null || true; "
        f"else "
        f"  echo 'NEED_CREATE'; "
        f"fi"
    )
    stdout, success = run_command(cmd, check=False, capture_output=True)
    return stdout, success


def ensure_keys_directory():
    """Убедиться, что директория keys существует и не является симлинком на char device"""
    # Проверяем, является ли keys симлинком
    check_symlink_cmd = (
        f"cd {PROJECT_ROOT} && "
        f"if [ -L keys ]; then "
        f"  echo 'SYMLINK'; "
        f"  readlink -f keys; "
        f"elif [ -d keys ]; then "
        f"  echo 'DIRECTORY'; "
        f"else "
        f"  echo 'NOT_EXISTS'; "
        f"fi"
    )
    symlink_output, _ = run_command(check_symlink_cmd, check=False, capture_output=True)
    
    # Если это симлинк, удаляем его и создаем настоящую директорию
    if "SYMLINK" in symlink_output:
        target_dir = symlink_output.split('\n')[1] if '\n' in symlink_output else ""
        print(f"  keys является симлинком на: {target_dir}")
        print(f"  Удаляю симлинк и создаю настоящую директорию...")
        
        # Удаляем симлинк
        remove_symlink_cmd = (
            f"cd {PROJECT_ROOT} && "
            f"rm -f keys && "
            f"mkdir -p keys && "
            f"chown boincadm:boincadm keys && "
            f"echo 'Symlink removed, directory created'"
        )
        run_command(remove_symlink_cmd, check=False, capture_output=False)
        
        # Копируем существующие ключи из secrets (кроме char device)
        if target_dir:
            copy_keys_cmd = (
                f"if [ -f {target_dir}/code_sign_public ] && [ ! -c {target_dir}/code_sign_public ]; then "
                f"  cp {target_dir}/code_sign_public {PROJECT_ROOT}/keys/ 2>/dev/null || true; "
                f"fi && "
                f"if [ -f {target_dir}/upload_private ] && [ ! -c {target_dir}/upload_private ]; then "
                f"  cp {target_dir}/upload_private {PROJECT_ROOT}/keys/ 2>/dev/null || true; "
                f"fi && "
                f"if [ -f {target_dir}/upload_public ] && [ ! -c {target_dir}/upload_public ]; then "
                f"  cp {target_dir}/upload_public {PROJECT_ROOT}/keys/ 2>/dev/null || true; "
                f"fi && "
                f"chown boincadm:boincadm {PROJECT_ROOT}/keys/* 2>/dev/null || true && "
                f"echo 'Keys copied'"
            )
            run_command(copy_keys_cmd, check=False, capture_output=False)
    else:
        # Убеждаемся, что директория существует
        cmd = (
            f"cd {PROJECT_ROOT} && "
            f"mkdir -p keys && "
            f"chown boincadm:boincadm keys 2>/dev/null || true && "
            f"echo 'Directory ready'"
        )
        run_command(cmd, check=False, capture_output=False)
    
    return True


def generate_keys():
    """Создать ключи подписи"""
    print("Генерация ключей подписи...")
    
    # Убеждаемся, что директория keys существует и не является симлинком
    if not ensure_keys_directory():
        print("✗ Ошибка: не удалось подготовить директорию keys", file=sys.stderr)
        return False
    
    # Теперь keys - настоящая директория, можно создавать ключи напрямую
    keys_dir = f"{PROJECT_ROOT}/keys"
    
    # Удаляем старые ключи, если они существуют (включая char device, если возможно)
    remove_old_keys_cmd = (
        f"cd {PROJECT_ROOT} && "
        f"# Пробуем удалить char device разными способами "
        f"if [ -c keys/code_sign_private ]; then "
        f"  rm -f keys/code_sign_private 2>&1 || "
        f"  unlink keys/code_sign_private 2>&1 || true; "
        f"fi; "
        f"rm -f keys/code_sign_private keys/code_sign_public 2>&1 || true; "
        f"echo 'Old keys removed'"
    )
    run_command(remove_old_keys_cmd, check=False, capture_output=False)
    
    # Генерируем ключ напрямую с правильными именами
    cmd = (
        f"cd {PROJECT_ROOT} && "
        f"bin/crypt_prog -genkey 1024 keys/code_sign_private keys/code_sign_public 2>&1 && "
        f"sync && "  # Принудительно сбрасываем буферы на диск
        f"chown boincadm:boincadm keys/code_sign_private keys/code_sign_public 2>/dev/null || true && "
        f"echo 'Keys generated'"
    )
    
    if not run_command(cmd, check=True, capture_output=False):
        print("✗ Ошибка при генерации ключей", file=sys.stderr)
        return False
    
    # Даем время файловой системе синхронизироваться
    time.sleep(1)
    
    return True


def verify_keys():
    """Проверить, что ключи созданы и не пустые"""
    cmd = (
        f"cd {PROJECT_ROOT} && "
        f"if [ -f keys/code_sign_private ] && [ -s keys/code_sign_private ] && "
        f"   [ -f keys/code_sign_public ] && [ -s keys/code_sign_public ]; then "
        f"  echo 'SUCCESS'; "
        f"  echo 'Private key:'; "
        f"  ls -lh keys/code_sign_private; "
        f"  echo 'Public key:'; "
        f"  ls -lh keys/code_sign_public; "
        f"  echo 'Key content (first 100 chars):'; "
        f"  head -c 100 keys/code_sign_private; "
        f"  echo ''; "
        f"else "
        f"  echo 'FAILED'; "
        f"  ls -la keys/ 2>&1 || true; "
        f"fi"
    )
    stdout, success = run_command(cmd, check=False, capture_output=True)
    return stdout, success


def generate_signing_keys():
    """Главная функция: проверить и создать ключи подписи при необходимости"""
    print("\n" + "=" * 80)
    print("ГЕНЕРАЦИЯ КЛЮЧЕЙ ПОДПИСИ КОДА")
    print("=" * 80)
    
    # Ждем создания проекта
    wait_for_project()
    
    # Проверяем существование ключа
    print("\nПроверка существования ключа подписи...")
    check_output, check_success = check_key_exists()
    
    if "EXISTS" in check_output:
        print("✓ Ключ подписи уже существует и не пустой")
        print(check_output)
        return True
    
    if "NEED_CREATE" not in check_output:
        print(f"⚠ Неожиданный результат проверки: {check_output}", file=sys.stderr)
    
    # Создаем ключ
    print("\nКлюч не найден или пустой. Создаю новый ключ...")
    if not generate_keys():
        return False
    
    # Проверяем результат
    print("\nПроверка созданных ключей...")
    verify_output, verify_success = verify_keys()
    
    if "SUCCESS" in verify_output:
        print("✓ Ключи подписи успешно созданы")
        print(verify_output)
        return True
    else:
        print("✗ Ключи не были созданы или пустые", file=sys.stderr)
        print(verify_output, file=sys.stderr)
        return False


def main():
    """Точка входа для запуска скрипта отдельно"""
    if not generate_signing_keys():
        sys.exit(1)
    
    print("\n" + "=" * 80)
    print("✓ ГЕНЕРАЦИЯ КЛЮЧЕЙ ЗАВЕРШЕНА УСПЕШНО")
    print("=" * 80)


if __name__ == "__main__":
    main()

