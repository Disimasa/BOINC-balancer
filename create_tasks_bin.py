#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Создание нативных (без boinc2docker) задач из Python-скриптов:
- Собираем бинарь через pyinstaller внутри контейнера apache
- Кладём бинарь в apps/<app>/1.0/x86_64-pc-linux-gnu/
- Создаём минимальные input/output templates
- Регистрируем приложение (xadd) и обновляем версии (update_versions)
- Создаём workunits через bin/create_work
"""
from __future__ import print_function
import subprocess
import sys
import argparse
import os
import threading
import time
import random

PROJECT_HOME = "/home/boincadm/project"
CONTAINER_NAME = "server-apache-1"

APP_CONFIGS = {
    "fast_task": {
        "script": "tasks/fast_task.py",
        "default_count": 100,
        "target_nresults": 2,
        "version_num": "100"
    },
    "medium_task": {
        "script": "tasks/medium_task.py",
        "default_count": 100,
        "target_nresults": 2,
        "version_num": "100"
    },
    "long_task": {
        "script": "tasks/long_task.py",
        "default_count": 100,
        "target_nresults": 2,
        "version_num": "100"
    },
    "random_task": {
        "script": "tasks/random_task.py",
        "default_count": 100,
        "target_nresults": 2,
        "version_num": "100"
    }
}


def run_command(cmd, check=True, capture_output=False):
    """Выполнить команду в контейнере apache и вывести stdout/stderr."""
    # Устанавливаем переменные окружения для BOINC
    env_cmd = "export BOINC_PROJECT_DIR={proj} && cd {proj} && {cmd}".format(
        proj=PROJECT_HOME, cmd=cmd
    )
    full_cmd = [
        "wsl.exe", "-e", "docker", "exec", CONTAINER_NAME,
        "bash", "-lc", env_cmd
    ]
    if not capture_output:
        print("Выполняю: {}".format(cmd))
    try:
        proc = subprocess.Popen(
            full_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        stdout, stderr = proc.communicate()
        
        if capture_output:
            # Возвращаем stdout как строку
            if check and proc.returncode != 0:
                # При ошибке возвращаем пустую строку
                return ""
            return stdout.strip() if stdout else ""
        else:
            # Выводим в консоль и возвращаем булево значение
            if stdout:
                print(stdout, end="")
            if stderr:
                print(stderr, file=sys.stderr, end="")
            if check and proc.returncode != 0:
                print("Ошибка: команда завершилась с кодом {}".format(proc.returncode), file=sys.stderr)
                return False
            return proc.returncode == 0
    except Exception as e:
        print("Исключение при выполнении команды: {}".format(e), file=sys.stderr)
        if capture_output:
            return ""
        return False


def check_file_exists(file_path):
    """Проверить существование файла внутри контейнера."""
    cmd = "test -f {}".format(file_path)
    return run_command(cmd, check=False)


def copy_file_to_container(host_path, container_path):
    """Скопировать файл с хоста в контейнер через docker cp."""
    if not os.path.exists(host_path):
        print("Файл на хосте не найден: {}".format(host_path), file=sys.stderr)
        return False
    try:
        # Преобразуем Windows путь в WSL путь
        abs_path = os.path.abspath(host_path)
        if abs_path.startswith('C:\\') or abs_path.startswith('c:\\'):
            wsl_path = '/mnt/c/' + abs_path[3:].replace('\\', '/')
        else:
            wsl_path = abs_path.replace('\\', '/')
        
        result = subprocess.run(
            ["wsl.exe", "-e", "docker", "cp", wsl_path, "{}:{}".format(CONTAINER_NAME, container_path)],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        return True
    except subprocess.CalledProcessError as e:
        stderr_msg = e.stderr.decode() if isinstance(e.stderr, bytes) else str(e.stderr)
        print("Ошибка при копировании {} в контейнер: {}".format(host_path, stderr_msg), file=sys.stderr)
        return False


def ensure_templates_and_placeholder(app_name):
    """Убедиться, что шаблоны и placeholder скопированы в контейнер."""
    # Определяем пути на хосте
    script_dir = os.path.dirname(os.path.abspath(__file__))
    host_tmpl_in = os.path.join(script_dir, "templates", "{}_in.xml".format(app_name))
    host_tmpl_out = os.path.join(script_dir, "templates", "{}_out.xml".format(app_name))
    host_placeholder = os.path.join(script_dir, "input_placeholder")
    
    # Пути в контейнере
    container_tmpl_in = os.path.join(PROJECT_HOME, "templates", "{}_in.xml".format(app_name))
    container_tmpl_out = os.path.join(PROJECT_HOME, "templates", "{}_out.xml".format(app_name))
    container_placeholder = os.path.join(PROJECT_HOME, "input_placeholder")
    
    # Копируем файлы, если их нет в контейнере
    if not check_file_exists(container_tmpl_in):
        if not copy_file_to_container(host_tmpl_in, container_tmpl_in):
            return False
    
    if not check_file_exists(container_tmpl_out):
        if not copy_file_to_container(host_tmpl_out, container_tmpl_out):
            return False
    
    if not check_file_exists(container_placeholder):
        if not copy_file_to_container(host_placeholder, container_placeholder):
            return False
    
    return True


def install_app_binary(app_name, binary_path, version_num):
    """Положить бинарь в apps/... и подготовить version.xml + шаблоны."""
    platform_dir = "apps/{}/1.0/x86_64-pc-linux-gnu".format(app_name)
    dest_dir = os.path.join(PROJECT_HOME, platform_dir)
    binary_name = "{}_bin".format(app_name)
    
    # Убеждаемся, что шаблоны и placeholder скопированы в контейнер
    if not ensure_templates_and_placeholder(app_name):
        print("Не удалось скопировать шаблоны и placeholder для {}".format(app_name), file=sys.stderr)
        return False, None, None, None
    
    # Используем шаблоны в каталоге templates и placeholder в корне проекта
    tmpl_in_rel = "templates/{}_in.xml".format(app_name)
    tmpl_out_rel = "templates/{}_out.xml".format(app_name)
    placeholder_rel = "input_placeholder"
    tmpl_in_path = os.path.join(PROJECT_HOME, tmpl_in_rel)
    tmpl_out_path = os.path.join(PROJECT_HOME, tmpl_out_rel)
    placeholder_path = os.path.join(PROJECT_HOME, placeholder_rel)

    # Проверяем наличие файлов ВНУТРИ КОНТЕЙНЕРА
    missing = []
    for p, desc in [
        (binary_path, "бинарный файл"),
        (tmpl_in_path, "шаблон ввода"),
        (tmpl_out_path, "шаблон вывода"),
        (placeholder_path, "placeholder файл")
    ]:
        if not check_file_exists(p):
            missing.append((p, desc))
    if missing:
        for p, desc in missing:
            print("Отсутствует файл ({desc}): {path}".format(desc=desc, path=p), file=sys.stderr)
        return False, None, None, None

    # Копируем бинарник
    cmd = (
        "mkdir -p {dest} && "
        "cp {bin} {dest}/{binary_name} && "
        "chmod +x {dest}/{binary_name}"
    ).format(dest=platform_dir, bin=binary_path, binary_name=binary_name)
    if not run_command(cmd, check=True):
        return False, None, None
    
    # Подписываем бинарник
    binary_full_path = os.path.join(platform_dir, binary_name)
    sig_path = os.path.join(platform_dir, "{}.sig".format(binary_name))
    
    # Пробуем несколько возможных путей к ключу
    # 1. keys/code_sign_private (основной путь)
    # 2. /run/secrets/keys/code_sign_private (если keys - симлинк)
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
    # Подписываем бинарник - это критично!
    # Если не удалось подписать, НЕ создаем пустой .sig файл,
    # чтобы update_versions мог подписать его автоматически
    sign_result = run_command(cmd_sign, check=False, capture_output=True)
    if not sign_result or "Signed with" not in sign_result:
        # Удаляем пустой .sig файл, если он был создан
        run_command(f"rm -f {sig_path}", check=False)
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
    run_command(version_xml, check=True)

    # Копируем placeholder в download/ для create_work
    # Шаблоны остаются в templates/ (create_work ожидает их там)
    placeholder_name = os.path.basename(placeholder_rel)
    tmpl_in_name = os.path.basename(tmpl_in_rel)
    tmpl_out_name = os.path.basename(tmpl_out_rel)
    
    # Создаём шаблоны БЕЗ расширения .xml (как требует create_work)
    tmpl_in_base = tmpl_in_name.replace('.xml', '')
    tmpl_out_base = tmpl_out_name.replace('.xml', '')
    cmd_create_templates = (
        "cp templates/{tmpl_in_xml} templates/{tmpl_in_base} && "
        "cp templates/{tmpl_out_xml} templates/{tmpl_out_base}"
    ).format(
        tmpl_in_xml=tmpl_in_name,
        tmpl_out_xml=tmpl_out_name,
        tmpl_in_base=tmpl_in_base,
        tmpl_out_base=tmpl_out_base
    )
    run_command(cmd_create_templates, check=True)
    
    # Используем stage_file для копирования входного файла в download/
    # Это правильный способ подготовки файлов для create_work
    cmd_stage = "bin/stage_file --copy {placeholder}".format(placeholder=placeholder_rel)
    run_command(cmd_stage, check=True)

    # Возвращаем имена файлов БЕЗ расширения .xml
    return True, tmpl_in_base, tmpl_out_base, placeholder_name


def ensure_download_hierarchy():
    """Создать иерархию download/00..ff для шаблонов и входных файлов."""
    cmd = (
        "mkdir -p download && "
        "for i in 0 1 2 3 4 5 6 7 8 9 a b c d e f; do "
        "  for j in 0 1 2 3 4 5 6 7 8 9 a b c d e f; do "
        "    mkdir -p download/$i$j; "
        "  done; "
        "done"
    )
    run_command(cmd, check=False)


def register_app(app_name, friendly_name=None):
    """Добавить приложение в БД (xadd), если его не было."""
    title = friendly_name or app_name
    cmd = "bin/xadd {app} \"{title}\" \"\"".format(app=app_name, title=title)
    run_command(cmd, check=False)  # допускаем, что уже существует


def update_versions():
    """Запустить update_versions с автоответом yes."""
    cmd = "yes | bin/update_versions 2>&1"
    result = run_command(cmd, check=False, capture_output=True)
    # Выводим результат для диагностики
    if result:
        # Показываем только важные строки
        lines = result.split('\n')
        for line in lines:
            if any(keyword in line.lower() for keyword in ['error', 'added', 'found', 'app version', 'signature']):
                if line.strip():
                    print(f"    {line}")
    return result


def get_app_version_id(app_name, version_num):
    """Получить app_version_id для приложения и версии из БД."""
    # version_num в БД хранится как число: 1.0 = 100, 1.1 = 101, и т.д.
    # Если version_num строка вида "100", преобразуем в int
    if isinstance(version_num, str):
        try:
            version_num_int = int(version_num)
        except ValueError:
            # Если это строка вида "1.0", преобразуем в 100
            parts = version_num.split('.')
            if len(parts) == 2:
                version_num_int = int(parts[0]) * 100 + int(parts[1])
            else:
                version_num_int = int(parts[0]) * 100
    else:
        version_num_int = version_num
    
    query = (
        "SELECT av.id FROM app_version av "
        "JOIN app a ON av.appid = a.id "
        "WHERE a.name = '{}' AND av.version_num = {} AND av.deprecated = 0 "
        "LIMIT 1"
    ).format(app_name, version_num_int)
    
    cmd = "mysql -u root -ppassword boincserver -N -e \"{}\"".format(query)
    output = run_command(cmd, capture_output=True)
    
    # output теперь всегда строка (может быть пустой)
    if output and isinstance(output, str) and output.strip().isdigit():
        return int(output.strip())
    return None


def ensure_app_version_exists(app_name, version_num):
    """Убедиться, что app_version существует в БД перед созданием задач.
    
    Если версия не найдена, выводит предупреждение и возвращает False.
    """
    app_version_id = get_app_version_id(app_name, version_num)
    if app_version_id is None:
        print(f"  ✗ ОШИБКА: app_version не найден для {app_name} версии {version_num}!", file=sys.stderr)
        print(f"     Это означает, что update_versions не был запущен или версия не была создана.", file=sys.stderr)
        print(f"     Задачи будут созданы с app_version_id=0 и не будут отправляться клиентам.", file=sys.stderr)
        return False
    return True


def create_workunits(app_name, count, target_nresults, tmpl_in_name, tmpl_out_name, placeholder_name, version_num, start_index=1):
    """Создать WU через bin/create_work.
    
    ВАЖНО: Перед вызовом этой функции необходимо убедиться, что update_versions() был запущен,
    чтобы app_version существовал в БД. create_work не ищет app_version_id автоматически,
    поэтому мы исправляем его после создания workunit.
    """
    import time
    
    # Получаем app_version_id ДО создания задач
    app_version_id = get_app_version_id(app_name, version_num)
    print('APP_VERSION_ID', app_version_id)
    if app_version_id is None:
        print(f"  ⚠ Предупреждение: app_version не найден для {app_name} версии {version_num}. "
              f"Задачи будут созданы с app_version_id=0.", file=sys.stderr)
    
    # Используем timestamp для уникальности имён задач
    timestamp = int(time.time())
    
    created_wu_names = []
    
    for idx in range(start_index, start_index + count):
        wu_name = "{app}_native_{ts}_{idx}".format(app=app_name, ts=timestamp, idx=idx)
        
        cmd = (
            "bin/create_work "
            "--appname {app} "
            "--app_version_num {ver} "
            "--wu_name {wu} "
            "--wu_template templates/{tmpl_in} "
            "--result_template templates/{tmpl_out} "
            "--target_nresults {tr} "
            "--min_quorum {mq} "
            "{placeholder}"
        ).format(
            app=app_name,
            ver=version_num,
            wu=wu_name,
            tmpl_in=tmpl_in_name,  # уже без расширения .xml
            tmpl_out=tmpl_out_name,  # уже без расширения .xml
            tr=target_nresults,
            mq=max(1, target_nresults),
            placeholder=placeholder_name,  # только имя файла, create_work найдёт его в download/
        )
        print("CMD", cmd)
        if run_command(cmd, check=False):
            created_wu_names.append(wu_name)
    
    # create_work НЕ устанавливает app_version_id автоматически
    # Нужно обновить его вручную через SQL UPDATE
    if app_version_id and created_wu_names:
        # Обновляем app_version_id для всех созданных workunits
        wu_names_str = "', '".join(created_wu_names)
        update_cmd = (
            "mysql -u root -ppassword boincserver -e "
            "\"UPDATE workunit w "
            "JOIN app a ON w.appid = a.id "
            "SET w.app_version_id = {av_id} "
            "WHERE a.name = '{app}' AND w.name IN ('{wu_names}') AND w.app_version_id = 0;\""
        ).format(av_id=app_version_id, app=app_name, wu_names=wu_names_str)
        update_result = run_command(update_cmd, check=False, capture_output=True)
        
        # Проверяем результат
        check_cmd = (
            "mysql -u root -ppassword boincserver -N -e "
            "\"SELECT COUNT(*) FROM workunit w "
            "JOIN app a ON w.appid = a.id "
            "WHERE a.name = '{app}' AND w.name IN ('{wu_names}') AND w.app_version_id = {av_id};\""
        ).format(av_id=app_version_id, app=app_name, wu_names=wu_names_str)
        check_output = run_command(check_cmd, capture_output=True)
        if check_output and check_output.strip().isdigit():
            count = int(check_output.strip())
            if count == len(created_wu_names):
                print(f"  ✓ Все {count} workunits имеют app_version_id = {app_version_id}")
            else:
                print(f"  ⚠ Только {count} из {len(created_wu_names)} workunits имеют app_version_id = {app_version_id}", file=sys.stderr)
        else:
            print(f"  ⚠ Не удалось проверить app_version_id для созданных workunits", file=sys.stderr)


def process_app(app_name, count, target_nresults):
    cfg = APP_CONFIGS[app_name]
    binary_path = os.path.join(PROJECT_HOME, "dist_bin", "{}_bin".format(app_name))
    ok_install, tmpl_in_rel, tmpl_out_rel, placeholder_rel = install_app_binary(app_name, binary_path, cfg["version_num"])
    if not ok_install or tmpl_in_rel is None or tmpl_out_rel is None:
        print("Не удалось установить бинарь для {}".format(app_name), file=sys.stderr)
        return False
    # register_app(app_name, friendly_name=app_name.replace("_", " ").title())
    create_workunits(app_name, count, target_nresults, tmpl_in_rel, tmpl_out_rel, placeholder_rel, cfg["version_num"])
    return True


def create_batch_of_tasks(batch_num, apps_config, target_nresults, app_templates):
    """Создать батч задач: по одной задаче каждого типа приложения в случайном порядке"""
    results = {}
    
    # Создаем список приложений и перемешиваем его для случайного порядка
    app_names = list(apps_config.keys())
    random.shuffle(app_names)
    
    for app_name in app_names:
        # Используем уже подготовленные шаблоны
        if app_name not in app_templates:
            results[app_name] = False
            continue
        
        cfg = apps_config[app_name]
        tmpl_in_rel, tmpl_out_rel, placeholder_rel = app_templates[app_name]
        
        # Регистрируем приложение (если еще не зарегистрировано)
        # register_app(app_name, friendly_name=app_name.replace("_", " ").title())
        
        # Создаем одну задачу для этого приложения
        create_workunits(app_name, 1, target_nresults, tmpl_in_rel, tmpl_out_rel, placeholder_rel, cfg["version_num"], start_index=batch_num)
        results[app_name] = True
    
    return results


def ensure_signing_key():
    """Убедиться, что ключ подписи существует и не пустой.
    
    Ключ должен быть создан в pipeline.py на этапе генерации ключей.
    """
    key_path = "keys/code_sign_private"
    
    # Проверяем, существует ли ключ, является ли он обычным файлом и не пустой ли он
    cmd_check = (
        "if [ -f {key} ] && [ ! -c {key} ] && [ -s {key} ]; then "
        "echo 'EXISTS'; "
        "else "
        "echo 'MISSING_OR_EMPTY'; "
        "fi"
    ).format(key=key_path)
    
    result = run_command(cmd_check, check=False, capture_output=True)
    
    if result and "EXISTS" in result:
        print(f"  ✓ Ключ подписи найден: {key_path}")
        return True
    else:
        print(f"  ✗ ОШИБКА: Ключ подписи не найден или пустой: {key_path}", file=sys.stderr)
        print(f"     Ключ должен быть создан в pipeline.py на этапе генерации ключей.", file=sys.stderr)
        print(f"     Запустите pipeline.py заново или создайте ключ вручную:", file=sys.stderr)
        print(f"     python3 generate_keys.py", file=sys.stderr)
        return False


def main():
    parser = argparse.ArgumentParser(description="Создание нативных задач из готовых бинарей (create_work)")
    parser.add_argument("--app", choices=list(APP_CONFIGS.keys()), help="Создать задачи только для одного приложения")
    parser.add_argument("--count", type=int, help="Количество задач (для одного app или для всех)")
    parser.add_argument("--target-nresults", type=int, default=2, help="Количество репликаций (по умолчанию 2)")
    args = parser.parse_args()

    ensure_download_hierarchy()
    
    # Проверяем ключ подписи - это критично!
    # if not ensure_signing_key():
    #     print("\n✗ ОШИБКА: Ключ подписи не найден! Невозможно создать задачи.", file=sys.stderr)
    #     print("  Ключ должен быть создан при создании проекта (make_project).", file=sys.stderr)
    #     return False

    targets = [args.app] if args.app else list(APP_CONFIGS.keys())

    # Если создаем для всех приложений - создаем батчами по 4 (по одной каждого типа)
    if not args.app and len(targets) > 1:
        # Сначала подготавливаем шаблоны для всех приложений
        print("\nПодготовка шаблонов для всех приложений...")
        app_templates = {}
        for app_name in targets:
            cfg = APP_CONFIGS[app_name]
            binary_path = os.path.join(PROJECT_HOME, "dist_bin", "{}_bin".format(app_name))
            ok_install, tmpl_in_rel, tmpl_out_rel, placeholder_rel = install_app_binary(
                app_name, binary_path, cfg["version_num"]
            )
            if ok_install and tmpl_in_rel and tmpl_out_rel:
                app_templates[app_name] = (tmpl_in_rel, tmpl_out_rel, placeholder_rel)
                print(f"  ✓ {app_name}: шаблоны подготовлены")
            else:
                print(f"  ✗ {app_name}: ошибка подготовки шаблонов", file=sys.stderr)
        
        if len(app_templates) != len(targets):
            print("✗ Не удалось подготовить шаблоны для всех приложений", file=sys.stderr)
            return False
        
        # Регистрируем все приложения
        # for app_name in targets:
        #     register_app(app_name, friendly_name=app_name.replace("_", " ").title())
        
        # Регистрируем приложения в БД (если еще не зарегистрированы)
        print("\nРегистрация приложений в БД (xadd)...")
        run_command("bin/xadd", check=False)
        
        # Обновляем версии ПЕРЕД созданием задач, чтобы app_version_id был доступен
        # Это критично, так как create_work использует app_version_num, но не ищет app_version_id автоматически
        # update_versions автоматически подпишет файлы, если .sig файлы отсутствуют
        print("\nОбновление версий приложений (необходимо для правильного app_version_id)...")
        print("  (update_versions автоматически подпишет файлы, если они не подписаны)")
        update_versions()
        
        # Даем время БД обновиться
        time.sleep(2)
        
        # Проверяем, что версии были созданы
        print("\nПроверка созданных версий...")
        all_versions_ok = True
        for app_name in targets:
            app_version_id = get_app_version_id(app_name, APP_CONFIGS[app_name]["version_num"])
            if app_version_id:
                print(f"  ✓ {app_name}: app_version_id = {app_version_id}")
            else:
                print(f"  ✗ {app_name}: версия не найдена в БД!", file=sys.stderr)
                all_versions_ok = False
        
        if not all_versions_ok:
            print("\n⚠ ВНИМАНИЕ: Не все версии были созданы!", file=sys.stderr)
            print("  Пробую повторно запустить update_versions...", file=sys.stderr)
            update_versions()
            time.sleep(2)
            
            # Повторная проверка
            print("\nПовторная проверка версий...")
            all_versions_ok = True
            for app_name in targets:
                app_version_id = get_app_version_id(app_name, APP_CONFIGS[app_name]["version_num"])
                if app_version_id:
                    print(f"  ✓ {app_name}: app_version_id = {app_version_id}")
                else:
                    print(f"  ✗ {app_name}: версия все еще не найдена!", file=sys.stderr)
                    all_versions_ok = False
        
        # Определяем количество задач на приложение
        count_per_app = args.count if args.count is not None else APP_CONFIGS[targets[0]]["default_count"]
        tr = args.target_nresults
        
        # Создаем задачи батчами: по одной задаче каждого типа в каждом батче
        print(f"\nСоздание {count_per_app} батчей задач (по одной каждого типа в каждом батче)...")
        batch_threads = []
        batch_errors = []
        
        def create_batch_thread(batch_num):
            try:
                results = create_batch_of_tasks(batch_num, APP_CONFIGS, tr, app_templates)
                for app_name, success in results.items():
                    if not success:
                        batch_errors.append((batch_num, app_name, "Ошибка создания задачи"))
            except Exception as e:
                batch_errors.append((batch_num, "unknown", str(e)))
        
        # Запускаем батчи параллельно (но ограничиваем количество одновременных потоков)
        max_concurrent_batches = 10  # Максимум 10 батчей одновременно
        active_threads = []
        
        for batch_num in range(1, count_per_app + 1):
            thread = threading.Thread(target=create_batch_thread, args=(batch_num,))
            thread.start()
            active_threads.append(thread)
            batch_threads.append(thread)
            
            # Если достигли лимита, ждем завершения одного из потоков
            if len(active_threads) >= max_concurrent_batches:
                active_threads[0].join()
                active_threads.pop(0)
            
            # Выводим прогресс каждые 50 батчей
            if batch_num % 50 == 0:
                print(f"  Создано батчей: {batch_num}/{count_per_app}")
        
        # Ждем завершения всех оставшихся потоков
        for thread in active_threads:
            thread.join()
        
        # Ждем завершения всех потоков
        for thread in batch_threads:
            thread.join()
        
        if batch_errors:
            print(f"\n⚠ Обнаружены ошибки при создании батчей: {len(batch_errors)}", file=sys.stderr)
            for batch_num, app_name, error in batch_errors[:10]:  # Показываем первые 10 ошибок
                print(f"  - Батч {batch_num}, {app_name}: {error}", file=sys.stderr)
        
        print(f"\n✓ Все {count_per_app} батчей созданы. Ожидание синхронизации с БД...")
        time.sleep(2)  # Даем время БД синхронизироваться
    else:
        # Для одного приложения - последовательно
        # Обновляем версии ПЕРЕД созданием задач
        print("\nОбновление версий приложений (необходимо для правильного app_version_id)...")
        update_versions()
        
        for app_name in targets:
            cfg = APP_CONFIGS[app_name]
            count = args.count if args.count is not None else cfg["default_count"]
            tr = args.target_nresults
            process_app(app_name, count, tr)
    
    # Перезапускаем feeder, чтобы он увидел новые задачи
    print("\nПерезапуск feeder для применения изменений...")
    run_command("bin/stop && sleep 2 && bin/start", check=False)
    
    print("\nГотово. Проверьте в интерфейсе количество задач и готовность к отправке.")


if __name__ == "__main__":
    main()

