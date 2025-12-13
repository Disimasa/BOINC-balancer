#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для создания Apps в BOINC и настройки валидаторов/ассимиляторов
Включает установку бинарных файлов и создание версий приложений
"""

from __future__ import print_function
import subprocess
import sys
import time
import os

PROJECT_HOME = "/home/boincadm/project"
CONTAINER_NAME = "server-apache-1"

apps = [
    {"name": "fast_task", "resultsdir": "/results/fast_task", "weight": 1.0},
    {"name": "medium_task", "resultsdir": "/results/medium_task", "weight": 1.0},
    {"name": "long_task", "resultsdir": "/results/long_task", "weight": 1.0},
    {"name": "random_task", "resultsdir": "/results/random_task", "weight": 1.0}
]


def run_cmd(cmd, check=True):
    """Выполнить команду в контейнере"""
    full_cmd = ["wsl.exe", "-e", "docker", "exec", CONTAINER_NAME, "bash", "-c", cmd]
    proc = subprocess.Popen(full_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    stdout, stderr = proc.communicate()
    if stdout:
        print(stdout, end='')
    if stderr:
        print(stderr, file=sys.stderr, end='')
    if check and proc.returncode != 0:
        print("Ошибка: команда завершилась с кодом {}".format(proc.returncode), file=sys.stderr)
        sys.exit(1)
    return proc.returncode == 0


def check_file_exists(file_path):
    """Проверить существование файла внутри контейнера."""
    cmd = "test -f {}".format(file_path)
    return run_cmd(cmd, check=False)


def create_app(app_name, resultsdir, weight=1.0):
    """Создать приложение"""
    print("\nСоздаю App: {} (weight={})".format(app_name, weight))
    
    # Структура директорий
    run_cmd("cd {} && mkdir -p apps/{}/1.0/x86_64-pc-linux-gnu".format(PROJECT_HOME, app_name), check=False)
    
    # Assimilator скрипт
    run_cmd("""cd {} && cat > bin/{}_assimilator << 'EOF'
#!/bin/bash
RESULTS_DIR={}
mkdir -p "$RESULTS_DIR"
for file in "$@"; do
    [ -f "$file" ] && cp "$file" "$RESULTS_DIR/"
done
EOF
chmod +x bin/{}_assimilator && chown boincadm:boincadm bin/{}_assimilator""".format(
        PROJECT_HOME, app_name, resultsdir, app_name, app_name
    ), check=False)
    
    # Добавляем в config.xml
    run_cmd("""cd {} && if ! grep -q 'sample_trivial_validator.*{}' config.xml; then
        sed -i '/<\\/daemons>/i\\        <daemon>\\n            <cmd>sample_trivial_validator -app {}</cmd>\\n        </daemon>' config.xml
    fi""".format(PROJECT_HOME, app_name, app_name), check=False)
    
    run_cmd("""cd {} && if ! grep -q '{}_assimilator' config.xml; then
        sed -i '/<\\/daemons>/i\\        <daemon>\\n            <cmd>script_assimilator --app {} --script "{}_assimilator files"</cmd>\\n        </daemon>' config.xml
    fi""".format(PROJECT_HOME, app_name, app_name, app_name), check=False)
    
    # Добавляем в project.xml
    run_cmd("""cd {} && if ! grep -q '<name>{}</name>' project.xml; then
        sed -i '/<\\/boinc>/i\\    <app>\\n        <name>{}</name>\\n        <user_friendly_name>{}</user_friendly_name>\\n    </app>\\n' project.xml
    fi""".format(PROJECT_HOME, app_name, app_name, app_name), check=False)
    
    # Добавляем в БД
    run_cmd("cd {} && bin/xadd".format(PROJECT_HOME), check=False)
    
    # Устанавливаем вес приложения через SQL (appmgr требует MySQLdb, которого может не быть)
    run_cmd("""cd {} && mysql -u root -ppassword boincserver -e "UPDATE app SET weight = {} WHERE name = '{}';" """.format(
        PROJECT_HOME, weight, app_name
    ), check=False)
    
    print("  ✓ Weight установлен: {}".format(weight))
    
    return True


def setup_daemons():
    """Настроить валидаторы и ассимиляторы (только настройка, без запуска)"""
    # Символические ссылки для ассимиляторов (script_assimilator использует ../bin/)
    apps_list = " ".join([app['name'] for app in apps])
    run_cmd("""cd {} && mkdir -p ../bin && for app in {}; do
        ln -sf {}/bin/${{app}}_assimilator ../bin/${{app}}_assimilator
    done""".format(PROJECT_HOME, apps_list, PROJECT_HOME), check=False)
    
    # Права доступа на директории результатов
    results_dirs = " ".join([app['resultsdir'] for app in apps])
    run_cmd("mkdir -p {} && chown -R boincadm:boincadm /results && chmod -R 755 /results".format(results_dirs), check=False)
    
    # Добавляем флаги --allapps и --random_order_db к feeder для перемешивания задач
    # --allapps: перемешивает задачи по приложениям пропорционально весам
    # --random_order_db: выбирает задачи в случайном порядке из БД
    # Обновляем команду feeder независимо от того, есть ли уже флаги
    run_cmd("""cd {} && sed -i 's|<cmd>feeder -d 3[^<]*</cmd>|<cmd>feeder -d 3 --allapps --random_order_db</cmd>|g' config.xml""".format(PROJECT_HOME), check=False)
    print("  ✓ Feeder настроен с флагами --allapps --random_order_db")
    
    # Перезапуск демонов (чтобы применить изменения в config.xml)
    run_cmd("cd {} && bin/stop && sleep 2 && bin/start".format(PROJECT_HOME), check=False)
    print("  ✓ Демоны перезапущены")
    
    # Валидаторы и ассимиляторы теперь запускаются отдельным шагом в pipeline
    print("  ℹ Валидаторы и ассимиляторы будут запущены отдельным шагом в pipeline")


def install_app_binary(app_name, binary_path, version_num="100"):
    """Установить бинарный файл приложения и создать version.xml."""
    print(f"  Устанавливаю бинарный файл для {app_name}...")
    platform_dir = "apps/{}/1.0/x86_64-pc-linux-gnu".format(app_name)
    binary_name = "{}_bin".format(app_name)
    
    # Проверяем наличие бинарного файла
    if not check_file_exists(binary_path):
        print(f"  ⚠ Предупреждение: бинарный файл не найден: {binary_path}", file=sys.stderr)
        print(f"     Версия приложения не будет создана до установки бинарного файла.", file=sys.stderr)
        return False
    
    # Копируем бинарник
    cmd = (
        "mkdir -p {dest} && "
        "cp {bin} {dest}/{binary_name} && "
        "chmod +x {dest}/{binary_name}"
    ).format(dest=platform_dir, bin=binary_path, binary_name=binary_name)
    if not run_cmd(cmd, check=False):
        print(f"  ✗ Ошибка при копировании бинарного файла для {app_name}", file=sys.stderr)
        return False
    
    # Подписываем бинарник
    binary_full_path = os.path.join(platform_dir, binary_name)
    sig_path = os.path.join(platform_dir, "{}.sig".format(binary_name))
    
    cmd_sign = (
        "bin_path='{bin_path}' && "
        "sig_path='{sig_path}' && "
        "key1='keys/code_sign_private' && "
        "key2='/run/secrets/keys/code_sign_private' && "
        "if [ -f \"$key1\" ] && [ ! -c \"$key1\" ] && [ -s \"$key1\" ]; then "
        "  bin/sign_executable \"$bin_path\" \"$key1\" > \"$sig_path\" 2>&1 && echo 'Signed with keys/code_sign_private'; "
        "elif [ -f \"$key2\" ] && [ ! -c \"$key2\" ] && [ -s \"$key2\" ]; then "
        "  bin/sign_executable \"$bin_path\" \"$key2\" > \"$sig_path\" 2>&1 && echo 'Signed with /run/secrets/keys/code_sign_private'; "
        "else "
        "  echo 'Warning: code_sign_private key not found, update_versions will try to sign it'; "
        "  rm -f \"$sig_path\"; "
        "fi"
    ).format(
        bin_path=binary_full_path,
        sig_path=sig_path
    )
    sign_result = run_cmd(cmd_sign, check=False)
    if not sign_result:
        run_cmd("rm -f {}".format(sig_path), check=False)
        print(f"  ⚠ Предупреждение: не удалось подписать {binary_name}. "
              f"update_versions попытается подписать его автоматически.", file=sys.stderr)
    
    # Создаем version.xml для платформы
    version_xml = (
        "cat > {dest}/version.xml <<EOF\n"
        "<version>\n"
        "  <app_name>{app}</app_name>\n"
        "  <version_num>{ver}</version_num>\n"
        "  <platform>x86_64-pc-linux-gnu</platform>\n"
        "  <file_ref>\n"
        "    <file_name>{binary}</file_name>\n"
        "    <main_program/>\n"
        "  </file_ref>\n"
        "</version>\n"
        "EOF"
    ).format(dest=platform_dir, app=app_name, ver=version_num, binary=binary_name)
    run_cmd(version_xml, check=False)
    
    print(f"  ✓ Бинарный файл установлен для {app_name}")
    return True


def update_versions():
    """Запустить update_versions с автоответом yes."""
    print("\nОбновление версий приложений...")
    cmd = "yes | bin/update_versions 2>&1"
    run_cmd(cmd, check=False)
    
    # Проверяем, что версии были созданы
    print("\nПроверка созданных версий...")
    for app in apps:
        app_name = app['name']
        version_num = 100  # 1.0 = 100
        query = (
            "SELECT av.id FROM app_version av "
            "JOIN app a ON av.appid = a.id "
            "WHERE a.name = '{}' AND av.version_num = {} AND av.deprecated = 0 "
            "LIMIT 1"
        ).format(app_name, version_num)
        
        check_cmd = "cd {} && mysql -u root -ppassword boincserver -N -e \"{}\"".format(PROJECT_HOME, query)
        proc = subprocess.Popen(
            ["wsl.exe", "-e", "docker", "exec", CONTAINER_NAME, "bash", "-c", check_cmd],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
        )
        stdout, _ = proc.communicate()
        if stdout and stdout.strip().isdigit():
            print(f"  ✓ {app_name}: app_version_id = {stdout.strip()}")
        else:
            print(f"  ✗ {app_name}: версия не найдена в БД!", file=sys.stderr)


def create_apps():
    """Создать все Apps"""
    print("=" * 60)
    print("Создание Apps...")
    print("=" * 60)
    
    for app in apps:
        weight = app.get('weight', 1.0)  # По умолчанию weight = 1.0
        create_app(app['name'], app['resultsdir'], weight)
    
    print("\n" + "=" * 60)
    print("Настройка валидаторов и ассимиляторов...")
    print("=" * 60)
    setup_daemons()
    
    # Шаблоны результатов
    for app in apps:
        run_cmd("""cd {}/templates && [ ! -f {}_out ] && cp boinc2docker_out {}_out""".format(
            PROJECT_HOME, app['name'], app['name']
        ), check=False)
    
    # Устанавливаем бинарные файлы и создаем версии приложений
    print("\n" + "=" * 60)
    print("Установка бинарных файлов и создание версий приложений...")
    print("=" * 60)
    
    binaries_installed = True
    for app in apps:
        app_name = app['name']
        binary_path = os.path.join(PROJECT_HOME, "dist_bin", "{}_bin".format(app_name))
        if not install_app_binary(app_name, binary_path, "100"):
            binaries_installed = False
    
    if binaries_installed:
        # Обновляем версии приложений в БД
        update_versions()
    else:
        print("\n⚠ Предупреждение: не все бинарные файлы были установлены.", file=sys.stderr)
        print("  Версии приложений не будут созданы до установки всех бинарных файлов.", file=sys.stderr)

    print("\n" + "=" * 60)
    print("✓ Все Apps созданы и настроены!")
    print("=" * 60)


if __name__ == "__main__":
    create_apps()
