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
    
    # Assimilator скрипт (оптимизирован для снижения I/O нагрузки)
    # Использует перемещение (mv) вместо копирования для уменьшения операций записи
    # И добавляет небольшую задержку для батчинга операций
    run_cmd("""cd {} && cat > bin/{}_assimilator << 'EOF'
#!/bin/bash
RESULTS_DIR={}
mkdir -p "$RESULTS_DIR"
# Используем перемещение вместо копирования для уменьшения I/O операций
# Файлы уже обработаны валидатором, оригиналы больше не нужны
for file in "$@"; do
    if [ -f "$file" ]; then
        # Используем mv вместо cp - это одна операция вместо двух (чтение + запись)
        mv "$file" "$RESULTS_DIR/" 2>/dev/null || cp "$file" "$RESULTS_DIR/"
    fi
done
EOF
chmod +x bin/{}_assimilator && chown boincadm:boincadm bin/{}_assimilator""".format(
        PROJECT_HOME, app_name, resultsdir, app_name, app_name
    ), check=False)
    
    # Добавляем в config.xml
    run_cmd("""cd {} && if ! grep -q 'sample_trivial_validator.*{}' config.xml; then
        sed -i '/<\\/daemons>/i\\        <daemon>\\n            <cmd>sample_trivial_validator -app {}</cmd>\\n        </daemon>' config.xml
    fi""".format(PROJECT_HOME, app_name, app_name), check=False)
    
    # Настраиваем ассимилятор с увеличенным интервалом для снижения I/O нагрузки
    # --sleep_interval 30: проверяет новые результаты каждые 30 секунд вместо 10
    # Это позволяет батчить операции записи и снизить нагрузку на диск
    run_cmd("""cd {} && if ! grep -q '{}_assimilator' config.xml; then
        sed -i '/<\\/daemons>/i\\        <daemon>\\n            <cmd>script_assimilator --app {} --script "{}_assimilator files" --sleep_interval 30</cmd>\\n        </daemon>' config.xml
    else
        # Обновляем существующий ассимилятор, добавляя sleep_interval если его нет
        sed -i 's|script_assimilator --app {} --script "{}_assimilator files"|script_assimilator --app {} --script "{}_assimilator files" --sleep_interval 30|g' config.xml
    fi""".format(PROJECT_HOME, app_name, app_name, app_name, app_name, app_name, app_name, app_name), check=False)
    
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
    
    # Добавляем флаги к feeder для перемешивания задач
    # --allapps: перемешивает задачи по приложениям пропорционально весам
    # --random_order_db: выбирает задачи в случайном порядке из БД
    # --by_batch: перемешивает задачи по батчам для более равномерного распределения
    # Обновляем команду feeder независимо от того, есть ли уже флаги
    run_cmd("""cd {} && sed -i 's|<cmd>feeder -d 3[^<]*</cmd>|<cmd>feeder -d 3 --allapps --random_order_db --by_batch</cmd>|g' config.xml""".format(PROJECT_HOME), check=False)
    print("  ✓ Feeder настроен с флагами --allapps --random_order_db --by_batch")
    
    # Устанавливаем max_wus_to_send = 1, max_wus_in_progress = 1 и max_ncpus = 1, чтобы за раз отправлялась только одна задача
    # max_wus_to_send = 1: базовое количество задач на CPU
    # max_wus_in_progress = 1: максимальное количество задач в процессе выполнения на хосте
    # max_ncpus = 1: ограничиваем количество CPU до 1 для расчета (чтобы mult = 1)
    # Результат: max_jobs_per_rpc = 1 * 1 = 1 задача за раз
    run_cmd("""cd {} && if ! grep -q '<max_wus_to_send>' config.xml; then
        sed -i '/<\\/boinc>/i\\    <max_wus_to_send>1</max_wus_to_send>' config.xml
    else
        sed -i 's|<max_wus_to_send>[0-9]*</max_wus_to_send>|<max_wus_to_send>1</max_wus_to_send>|g' config.xml
    fi""".format(PROJECT_HOME), check=False)
    run_cmd("""cd {} && if ! grep -q '<max_wus_in_progress>' config.xml; then
        sed -i '/<\\/boinc>/i\\    <max_wus_in_progress>1</max_wus_in_progress>' config.xml
    else
        sed -i 's|<max_wus_in_progress>[0-9]*</max_wus_in_progress>|<max_wus_in_progress>1</max_wus_in_progress>|g' config.xml
    fi""".format(PROJECT_HOME), check=False)
    run_cmd("""cd {} && if ! grep -q '<max_ncpus>' config.xml; then
        sed -i '/<\\/boinc>/i\\    <max_ncpus>1</max_ncpus>' config.xml
    else
        sed -i 's|<max_ncpus>[0-9]*</max_ncpus>|<max_ncpus>1</max_ncpus>|g' config.xml
    fi""".format(PROJECT_HOME), check=False)
    print("  ✓ Установлено max_wus_to_send = 1, max_wus_in_progress = 1 и max_ncpus = 1 (одна задача за раз независимо от CPU)")
    
    # Устанавливаем min_sendwork_interval = 2 (минимальный интервал между отправками работы клиенту в секундах)
    run_cmd("""cd {} && if ! grep -q '<min_sendwork_interval>' config.xml; then
        sed -i '/<\\/boinc>/i\\    <min_sendwork_interval>2</min_sendwork_interval>' config.xml
    else
        sed -i 's|<min_sendwork_interval>[0-9]*</min_sendwork_interval>|<min_sendwork_interval>2</min_sendwork_interval>|g' config.xml
    fi""".format(PROJECT_HOME), check=False)
    print("  ✓ Установлено min_sendwork_interval = 2 (минимальный интервал между отправками работы)")
    
    # Включаем enable_assignment для поддержки --target_host
    run_cmd("""cd {} && if ! grep -q '<enable_assignment>' config.xml; then
        sed -i '/<\\/boinc>/i\\    <enable_assignment/>' config.xml
    fi""".format(PROJECT_HOME), check=False)
    print("  ✓ Включено enable_assignment для поддержки назначения задач хостам")
    
    # Включаем debug_assignment и debug_send для отладки назначенных задач
    run_cmd("""cd {} && if ! grep -q '<debug_assignment>' config.xml; then
        sed -i '/<\\/boinc>/i\\    <debug_assignment/>' config.xml
    fi""".format(PROJECT_HOME), check=False)
    print("  ✓ Включено debug_assignment для отладки назначенных задач")
    
    run_cmd("""cd {} && if ! grep -q '<debug_send>' config.xml; then
        sed -i '/<\\/boinc>/i\\    <debug_send/>' config.xml
    fi""".format(PROJECT_HOME), check=False)
    print("  ✓ Включено debug_send для отладки отправки задач")
    
    # Устанавливаем max_jobs_in_progress = 1 в project_prefs для всех клиентов
    # Это ограничивает количество задач, которые клиент может выполнять одновременно
    script_dir = os.path.dirname(os.path.abspath(__file__))
    set_max_jobs_script = os.path.join(script_dir, "set_client_max_jobs.py")
    if os.path.exists(set_max_jobs_script):
        import subprocess
        result = subprocess.run(
            [sys.executable, set_max_jobs_script],
            capture_output=True,
            text=True,
            timeout=60
        )
        if result.returncode == 0:
            print("  ✓ Установлено max_jobs_in_progress = 1 в project_prefs для всех клиентов")
        else:
            print(f"  ⚠ Предупреждение: не удалось установить max_jobs_in_progress: {result.stderr[:200]}", file=sys.stderr)
    
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
