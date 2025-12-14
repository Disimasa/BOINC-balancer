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
        "default_count": 1000,
        "target_nresults": 1,
        "version_num": "100"
    },
    "medium_task": {
        "script": "tasks/medium_task.py",
        "default_count": 1000,
        "target_nresults": 1,
        "version_num": "100"
    },
    "random_task": {
        "script": "tasks/random_task.py",
        "default_count": 1000,
        "target_nresults": 1,
        "version_num": "100"
    },
    "long_task": {
        "script": "tasks/long_task.py",
        "default_count": 1000,
        "target_nresults": 1,
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
            stderr=subprocess.PIPE
        )
        stdout_bytes, stderr_bytes = proc.communicate()
        
        # Декодируем с обработкой ошибок кодировки
        try:
            stdout = stdout_bytes.decode('utf-8', errors='replace') if stdout_bytes else ""
            stderr = stderr_bytes.decode('utf-8', errors='replace') if stderr_bytes else ""
        except Exception as decode_error:
            # Если не удалось декодировать, используем latin-1 как fallback
            stdout = stdout_bytes.decode('latin-1', errors='replace') if stdout_bytes else ""
            stderr = stderr_bytes.decode('latin-1', errors='replace') if stderr_bytes else ""
        
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
    # Нормализуем путь: заменяем обратные слеши на прямые для Linux путей
    normalized_path = file_path.replace('\\', '/')
    cmd = "test -f {}".format(normalized_path)
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
    
    # Пути в контейнере (используем прямые слеши для Linux путей)
    container_tmpl_in = "{}/templates/{}_in.xml".format(PROJECT_HOME, app_name)
    container_tmpl_out = "{}/templates/{}_out.xml".format(PROJECT_HOME, app_name)
    container_placeholder = "{}/input_placeholder".format(PROJECT_HOME)
    
    # Копируем файлы, если их нет в контейнере
    if not check_file_exists(container_tmpl_in):
        if not copy_file_to_container(host_tmpl_in, container_tmpl_in):
            return False
    
    if not check_file_exists(container_tmpl_out):
        if not copy_file_to_container(host_tmpl_out, container_tmpl_out):
            return False
    
    # Placeholder файл больше не требуется для sleep-задач
    # if not check_file_exists(container_placeholder):
    #     if not copy_file_to_container(host_placeholder, container_placeholder):
    #         return False
    
    return True


def install_app_binary(app_name, binary_path, version_num):
    """Положить бинарь в apps/... и подготовить version.xml + шаблоны."""
    platform_dir = "apps/{}/1.0/x86_64-pc-linux-gnu".format(app_name)
    dest_dir = "{}/{}".format(PROJECT_HOME, platform_dir)
    binary_name = "{}_bin".format(app_name)
    
    # Убеждаемся, что шаблоны и placeholder скопированы в контейнер
    if not ensure_templates_and_placeholder(app_name):
        print("Не удалось скопировать шаблоны и placeholder для {}".format(app_name), file=sys.stderr)
        return False, None, None, None
    
    # Используем шаблоны в каталоге templates
    # Для простых задач (sleep) входные файлы не требуются
    tmpl_in_rel = "templates/{}_in.xml".format(app_name)
    tmpl_out_rel = "templates/{}_out.xml".format(app_name)
    placeholder_rel = None  # Не используем входные файлы для sleep-задач
    # Используем прямые слеши для Linux путей внутри контейнера
    tmpl_in_path = "{}/{}".format(PROJECT_HOME, tmpl_in_rel)
    tmpl_out_path = "{}/{}".format(PROJECT_HOME, tmpl_out_rel)
    # Проверяем наличие файлов ВНУТРИ КОНТЕЙНЕРА
    missing = []
    check_files = [
        (binary_path, "бинарный файл"),
        (tmpl_in_path, "шаблон ввода"),
        (tmpl_out_path, "шаблон вывода")
    ]
    # Добавляем placeholder только если он используется
    if placeholder_rel:
        placeholder_path = "{}/{}".format(PROJECT_HOME, placeholder_rel)
        check_files.append((placeholder_path, "placeholder файл"))
    
    for p, desc in check_files:
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

    # Определяем имя placeholder (если используется)
    # Шаблоны остаются в templates/ (create_work ожидает их там)
    placeholder_name = os.path.basename(placeholder_rel) if placeholder_rel else None
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
    
    # Если placeholder_name указан, размещаем файл в download/
    # Для задач без входных файлов (например, sleep-задачи) это не требуется
    if placeholder_name:
        # Используем stage_file для копирования входного файла в download/
        # Это правильный способ подготовки файлов для create_work
        # stage_file копирует файл в download/XX/ на основе MD5 имени файла
        # Важно: файл должен быть в корне проекта (не в templates/)
        cmd_stage = "bin/stage_file --copy --verbose {placeholder}".format(placeholder=placeholder_rel)
        if not run_command(cmd_stage, check=False):
            print(f"  ⚠ Предупреждение: stage_file вернул ошибку для {placeholder_rel}, но продолжаем", file=sys.stderr)
            # Проверяем, может файл уже есть в download/
            check_cmd = "find download -name 'input_placeholder' -type f 2>/dev/null | head -1"
            check_result = run_command(check_cmd, check=False, capture_output=True)
            if not check_result:
                print(f"  ✗ Файл {placeholder_rel} не найден в download/", file=sys.stderr)
                return False, None, None, None
    else:
        print("  ℹ Входные файлы не требуются для этого типа задач")

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


def update_versions():
    """Запустить update_versions с автоответом yes."""
    cmd = "yes | bin/update_versions 2>&1"
    return run_command(cmd, check=False)


def register_app(app_name, friendly_name=None):
    """Добавить приложение в БД (xadd), если его не было."""
    title = friendly_name or app_name
    cmd = "bin/xadd {app} \"{title}\" \"\"".format(app=app_name, title=title)
    run_command(cmd, check=False)  # допускаем, что уже существует


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


def create_workunits(app_name, count, target_nresults, tmpl_in_name, tmpl_out_name, placeholder_name, version_num, start_index=1, target_host_id=None):
    """Создать WU через bin/create_work.
    
    Args:
        app_name: имя приложения
        count: количество задач
        target_nresults: количество репликаций
        tmpl_in_name: имя шаблона входного файла
        tmpl_out_name: имя шаблона выходного файла
        placeholder_name: имя placeholder файла
        version_num: номер версии
        start_index: начальный индекс для имен задач
        target_host_id: ID хоста для назначения задачи (None = без назначения)
    
    Returns:
        список созданных workunit names для последующего обновления app_version_id.
    """
    import time
    
    # Используем timestamp для уникальности имён задач
    timestamp = int(time.time())
    
    created_wu_names = []
    
    for idx in range(start_index, start_index + count):
        wu_name = "{app}_native_{ts}_{idx}".format(app=app_name, ts=timestamp, idx=idx)
        
        # Добавляем --target_host если указан
        target_host_param = ""
        if target_host_id is not None:
            target_host_param = "--target_host {}".format(target_host_id)
        
        # Создаем команду create_work
        # Если placeholder_name указан, добавляем его как входной файл
        # Иначе создаем задачи без входных файлов (для простых sleep-задач)
        placeholder_param = ""
        if placeholder_name:
            placeholder_param = placeholder_name  # только имя файла, create_work найдёт его в download/
        
        cmd = (
            "bin/create_work "
            "--appname {app} "
            "--app_version_num {ver} "
            "--wu_name {wu} "
            "--wu_template templates/{tmpl_in} "
            "--result_template templates/{tmpl_out} "
            "--target_nresults {tr} "
            "--min_quorum {mq} "
            "{target_host} "
            "{placeholder}"
        ).format(
            app=app_name,
            ver=version_num,
            wu=wu_name,
            tmpl_in=tmpl_in_name,  # уже без расширения .xml
            tmpl_out=tmpl_out_name,  # уже без расширения .xml
            tr=target_nresults,
            mq=max(1, target_nresults),
            target_host=target_host_param,
            placeholder=placeholder_param,  # пустая строка, если входных файлов нет
        )
        if run_command(cmd, check=False):
            created_wu_names.append((app_name, wu_name))
    
    return created_wu_names


def process_app(app_name, count, target_nresults):
    """Создать задачи для одного приложения. Возвращает список (app_name, wu_name) созданных задач."""
    cfg = APP_CONFIGS[app_name]
    binary_path = "{}/dist_bin/{}_bin".format(PROJECT_HOME, app_name)
    ok_install, tmpl_in_rel, tmpl_out_rel, placeholder_rel = install_app_binary(app_name, binary_path, cfg["version_num"])
    if not ok_install or tmpl_in_rel is None or tmpl_out_rel is None:
        print("Не удалось установить бинарь для {}".format(app_name), file=sys.stderr)
        return []
    # register_app(app_name, friendly_name=app_name.replace("_", " ").title())
    return create_workunits(app_name, count, target_nresults, tmpl_in_rel, tmpl_out_rel, placeholder_rel, cfg["version_num"])


def get_active_hosts():
    """Получить список активных хостов из БД.
    
    Returns:
        list: список словарей с информацией о хостах [{'id': int, 'domain_name': str, 'task_count': int}, ...]
    """
    query = """
    SELECT 
        h.id as host_id,
        h.domain_name,
        COUNT(DISTINCT r.id) as task_count
    FROM host h
    LEFT JOIN result r ON h.id = r.hostid
    WHERE h.id > 0
    GROUP BY h.id, h.domain_name
    ORDER BY h.id;
    """
    
    cmd = "mysql -u root -ppassword boincserver -N -e \"{}\"".format(query)
    output = run_command(cmd, capture_output=True)
    
    if not output:
        return []
    
    hosts = []
    for line in output.strip().split('\n'):
        if not line.strip():
            continue
        parts = line.split('\t')
        if len(parts) >= 3:
            try:
                host_id = int(parts[0].strip())
                domain_name = parts[1].strip()
                task_count = int(parts[2].strip())
                hosts.append({
                    'id': host_id,
                    'domain_name': domain_name,
                    'task_count': task_count
                })
            except (ValueError, IndexError):
                continue
    
    return hosts


def create_batch_of_tasks(batch_num, apps_config, target_nresults, app_templates, hosts=None):
    """Создать батч задач: по одной задаче каждого типа приложения в случайном порядке.
    
    Args:
        batch_num: номер батча
        apps_config: конфигурация приложений
        target_nresults: количество репликаций
        app_templates: словарь шаблонов для приложений
        hosts: список хостов для балансировки (None = без балансировки)
    
    Returns:
        список кортежей (app_name, wu_name) созданных задач.
    """
    created_tasks = []
    
    # Создаем список приложений и перемешиваем его для случайного порядка
    app_names = list(apps_config.keys())
    random.shuffle(app_names)
    
    # Если хосты переданы, распределяем задачи по хостам
    host_idx = 0
    sorted_hosts = None
    if hosts:  # hosts может быть None или пустым списком - только тогда используем балансировку
        # Сортируем хосты по количеству задач (меньше задач = выше приоритет)
        sorted_hosts = sorted(hosts, key=lambda x: x['task_count'])
        # Распределяем задачи батча по хостам (round-robin)
        host_idx = 0
    
    for app_name in app_names:
        # Используем уже подготовленные шаблоны
        if app_name not in app_templates:
            continue
        
        cfg = apps_config[app_name]
        tmpl_in_rel, tmpl_out_rel, placeholder_rel = app_templates[app_name]
        
        # Определяем хост для задачи (если есть хосты)
        target_host_id = None
        if hosts:
            target_host_id = sorted_hosts[host_idx % len(sorted_hosts)]['id']
            host_idx += 1
        
        # Создаем одну задачу для этого приложения
        batch_tasks = create_workunits(app_name, 1, target_nresults, tmpl_in_rel, tmpl_out_rel, placeholder_rel, cfg["version_num"], start_index=batch_num, target_host_id=target_host_id)
        created_tasks.extend(batch_tasks)
    
    return created_tasks


def update_app_version_ids_batch(all_created_tasks):
    """Обновить app_version_id для всех созданных задач одной транзакцией.
    
    all_created_tasks: список кортежей (app_name, wu_name)
    """
    if not all_created_tasks:
        return
    
    # Группируем задачи по приложению
    tasks_by_app = {}
    for app_name, wu_name in all_created_tasks:
        if app_name not in tasks_by_app:
            tasks_by_app[app_name] = []
        tasks_by_app[app_name].append(wu_name)
    
    # Получаем app_version_id для каждого приложения
    app_version_map = {}
    for app_name in tasks_by_app.keys():
        cfg = APP_CONFIGS[app_name]
        app_version_id = get_app_version_id(app_name, cfg["version_num"])
        if app_version_id:
            app_version_map[app_name] = app_version_id
        else:
            print(f"  ⚠ Предупреждение: app_version не найден для {app_name}. "
                  f"Задачи будут иметь app_version_id=0.", file=sys.stderr)
    
    # Создаем SQL для обновления всех задач одной транзакцией
    update_statements = []
    for app_name, wu_names in tasks_by_app.items():
        if app_name not in app_version_map:
            continue
        
        app_version_id = app_version_map[app_name]
        wu_names_str = "', '".join(wu_names)
        update_statements.append(
            "UPDATE workunit w "
            "JOIN app a ON w.appid = a.id "
            "SET w.app_version_id = {} "
            "WHERE a.name = '{}' AND w.name IN ('{}') AND w.app_version_id = 0;".format(
                app_version_id, app_name, wu_names_str
            )
        )
    
    if not update_statements:
        print("  ⚠ Нет задач для обновления app_version_id", file=sys.stderr)
        return
    
    # Выполняем все обновления одной транзакцией
    print("\nОбновление app_version_id для всех созданных задач...")
    
    # Формируем полный SQL с транзакцией
    sql_lines = ["START TRANSACTION;"] + update_statements + ["COMMIT;"]
    sql_content = "\n".join(sql_lines)
    
    # Выполняем SQL через stdin напрямую в docker exec
    # Это более надежный способ, чем heredoc через run_command
    try:
        full_cmd = [
            "wsl.exe", "-e", "docker", "exec", "-i", CONTAINER_NAME,
            "bash", "-c", "cd {} && mysql -u root -ppassword boincserver".format(PROJECT_HOME)
        ]
        proc = subprocess.Popen(
            full_cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        stdout_bytes, stderr_bytes = proc.communicate(input=sql_content.encode('utf-8'))
        
        # Декодируем вывод
        try:
            stdout = stdout_bytes.decode('utf-8', errors='replace') if stdout_bytes else ""
            stderr = stderr_bytes.decode('utf-8', errors='replace') if stderr_bytes else ""
        except Exception:
            stdout = stdout_bytes.decode('latin-1', errors='replace') if stdout_bytes else ""
            stderr = stderr_bytes.decode('latin-1', errors='replace') if stderr_bytes else ""
        
        result = proc.returncode == 0
        if stderr and not result:
            print(f"  ⚠ SQL ошибка: {stderr[:200]}", file=sys.stderr)
    except Exception as e:
        print(f"  ⚠ Ошибка при выполнении SQL: {e}", file=sys.stderr)
        result = False
    
    if result:
        total_updated = sum(len(wu_names) for wu_names in tasks_by_app.values())
        print(f"  ✓ Обновлено app_version_id для {total_updated} задач")
        
        # Проверяем результат
        for app_name, wu_names in tasks_by_app.items():
            if app_name not in app_version_map:
                continue
            app_version_id = app_version_map[app_name]
            wu_names_str = "', '".join(wu_names)
            # Экранируем кавычки для SQL
            wu_names_escaped = wu_names_str.replace("'", "\\'")
            check_sql = (
                "SELECT COUNT(*) FROM workunit w "
                "JOIN app a ON w.appid = a.id "
                "WHERE a.name = '{}' AND w.name IN ('{}') AND w.app_version_id = {};"
            ).format(app_name, wu_names_escaped, app_version_id)
            
            # Выполняем проверку через stdin
            try:
                full_cmd = [
                    "wsl.exe", "-e", "docker", "exec", "-i", CONTAINER_NAME,
                    "bash", "-c", "cd {} && mysql -u root -ppassword boincserver -N".format(PROJECT_HOME)
                ]
                proc = subprocess.Popen(
                    full_cmd,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
                stdout_bytes, _ = proc.communicate(input=check_sql.encode('utf-8'))
                check_output = stdout_bytes.decode('utf-8', errors='replace') if stdout_bytes else ""
            except Exception:
                check_output = None
            if check_output and check_output.strip().isdigit():
                count = int(check_output.strip())
                if count == len(wu_names):
                    print(f"    ✓ {app_name}: {count} задач имеют app_version_id = {app_version_id}")
                else:
                    print(f"    ⚠ {app_name}: только {count} из {len(wu_names)} задач имеют app_version_id = {app_version_id}", file=sys.stderr)
    else:
        print("  ⚠ Ошибка при обновлении app_version_id", file=sys.stderr)


def create_host_app_versions_for_assigned(all_created_tasks):
    """Создать host_app_version записи для всех хостов и всех app_versions.
    
    Это критично для работы scheduler - без этих записей get_app_version
    не сможет найти версию приложения для хоста и задачи не будут отправлены.
    
    Создаем записи для ВСЕХ хостов и ВСЕХ app_versions, чтобы гарантировать,
    что назначенные задачи смогут быть отправлены.
    
    all_created_tasks: список кортежей (app_name, wu_name) - не используется, но оставлен для совместимости
    """
    # SQL для создания host_app_version записей для всех хостов и всех app_versions
    sql = """
    INSERT IGNORE INTO host_app_version (
        host_id,
        app_version_id,
        pfc_n,
        pfc_avg,
        et_n,
        et_avg,
        et_var,
        et_q,
        max_jobs_per_day,
        n_jobs_today,
        turnaround_n,
        turnaround_avg,
        turnaround_var,
        turnaround_q,
        consecutive_valid
    )
    SELECT DISTINCT
        h.id as host_id,
        av.id as app_version_id,
        0.0 as pfc_n,
        0.0 as pfc_avg,
        0.0 as et_n,
        0.0 as et_avg,
        0.0 as et_var,
        0.0 as et_q,
        100 as max_jobs_per_day,
        0 as n_jobs_today,
        0.0 as turnaround_n,
        0.0 as turnaround_avg,
        0.0 as turnaround_var,
        0.0 as turnaround_q,
        0 as consecutive_valid
    FROM host h
    CROSS JOIN app_version av
    JOIN app a ON av.appid = a.id
    WHERE h.id > 0
        AND a.deprecated = 0
        AND av.deprecated = 0
        AND a.name IN ('fast_task', 'medium_task', 'long_task', 'random_task')
        AND NOT EXISTS (
            SELECT 1 FROM host_app_version hav 
            WHERE hav.host_id = h.id 
            AND hav.app_version_id = av.id
        );
    """
    
    try:
        full_cmd = [
            "wsl.exe", "-e", "docker", "exec", "-i", CONTAINER_NAME,
            "bash", "-c", "cd {} && mysql -u root -ppassword boincserver".format(PROJECT_HOME)
        ]
        proc = subprocess.Popen(
            full_cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        stdout_bytes, stderr_bytes = proc.communicate(input=sql.encode('utf-8'))
        
        if proc.returncode == 0:
            # Проверяем, сколько записей было создано
            check_sql = """
            SELECT COUNT(*) FROM host_app_version hav;
            """
            
            proc2 = subprocess.Popen(
                full_cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            stdout_bytes2, _ = proc2.communicate(input=check_sql.encode('utf-8'))
            output = stdout_bytes2.decode('utf-8', errors='replace').strip() if stdout_bytes2 else ""
            # Парсим вывод: первая строка - заголовок, вторая - значение
            lines = [line.strip() for line in output.split('\n') if line.strip()]
            count = 0
            if len(lines) > 1:
                try:
                    count = int(lines[1])
                except (ValueError, IndexError):
                    # Если формат другой, пробуем взять последнее число
                    for line in reversed(lines):
                        try:
                            count = int(line)
                            break
                        except ValueError:
                            continue
            print(f"  ✓ Создано/обновлено {count} записей в host_app_version для всех хостов и app_versions")
        else:
            stderr = stderr_bytes.decode('utf-8', errors='replace') if stderr_bytes else ""
            print(f"  ⚠ Ошибка при создании host_app_version: {stderr[:200]}", file=sys.stderr)
    except Exception as e:
        print(f"  ⚠ Ошибка при создании host_app_version: {e}", file=sys.stderr)


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
    parser.add_argument("--target-nresults", type=int, default=1, help="Количество репликаций (по умолчанию 1)")
    parser.add_argument("--balance-hosts", action="store_true", help="Балансировать задачи между хостами (по умолчанию отключено)")
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
        # ВАЖНО: Бинарные файлы и версии приложений должны быть установлены в create_apps.py
        # Здесь мы только подготавливаем шаблоны для создания задач
        print("\nПодготовка шаблонов для всех приложений...")
        app_templates = {}
        for app_name in targets:
            # Убеждаемся, что шаблоны и placeholder скопированы в контейнер
            if not ensure_templates_and_placeholder(app_name):
                print(f"  ✗ {app_name}: ошибка подготовки шаблонов", file=sys.stderr)
                continue
            
            # Используем шаблоны в каталоге templates и placeholder в корне проекта
            tmpl_in_rel = "templates/{}_in.xml".format(app_name)
            tmpl_out_rel = "templates/{}_out.xml".format(app_name)
            placeholder_rel = None  # Не используем входные файлы для sleep-задач
            
            # Создаём шаблоны БЕЗ расширения .xml (как требует create_work)
            tmpl_in_base = os.path.basename(tmpl_in_rel).replace('.xml', '')
            tmpl_out_base = os.path.basename(tmpl_out_rel).replace('.xml', '')
            cmd_create_templates = (
                "cp templates/{} templates/{} && "
                "cp templates/{} templates/{}"
            ).format(
                os.path.basename(tmpl_in_rel), tmpl_in_base,
                os.path.basename(tmpl_out_rel), tmpl_out_base
            )
            if run_command(cmd_create_templates, check=False):
                # Для простых задач (sleep) входные файлы не требуются
                placeholder_name = None
                if placeholder_rel:
                    # Используем stage_file для копирования входного файла в download/
                    # stage_file копирует файл в download/XX/ на основе MD5 имени файла
                    cmd_stage = "bin/stage_file --copy --verbose {}".format(placeholder_rel)
                    if run_command(cmd_stage, check=False):
                        placeholder_name = os.path.basename(placeholder_rel)
                        print(f"  ✓ {app_name}: шаблоны подготовлены")
                    else:
                        # Проверяем, может файл уже есть в download/
                        check_cmd = "find download -name 'input_placeholder' -type f 2>/dev/null | head -1"
                        check_result = run_command(check_cmd, check=False, capture_output=True)
                        if check_result:
                            print(f"  ✓ {app_name}: файл уже существует в download/, используем его")
                            placeholder_name = os.path.basename(placeholder_rel)
                        else:
                            print(f"  ✗ {app_name}: ошибка при stage_file и файл не найден в download/", file=sys.stderr)
                else:
                    placeholder_name = None
                    print(f"  ✓ {app_name}: шаблоны подготовлены (без входных файлов)")
                
                if placeholder_name is not None or placeholder_rel is None:
                    app_templates[app_name] = (tmpl_in_base, tmpl_out_base, placeholder_name)
            else:
                print(f"  ✗ {app_name}: ошибка создания шаблонов", file=sys.stderr)
        
        if len(app_templates) != len(targets):
            print("✗ Не удалось подготовить шаблоны для всех приложений", file=sys.stderr)
            return False
        
        # Регистрируем все приложения
        # for app_name in targets:
        #     register_app(app_name, friendly_name=app_name.replace("_", " ").title())
        
        # ВАЖНО: Бинарные файлы и версии приложений должны быть установлены в create_apps.py
        # Здесь мы только проверяем, что версии существуют
        print("\nПроверка версий приложений (должны быть созданы в create_apps.py)...")
        all_versions_ok = True
        for app_name in targets:
            app_version_id = get_app_version_id(app_name, APP_CONFIGS[app_name]["version_num"])
            if app_version_id:
                print(f"  ✓ {app_name}: app_version_id = {app_version_id}")
            else:
                print(f"  ✗ {app_name}: версия не найдена в БД!", file=sys.stderr)
                print(f"     Убедитесь, что create_apps.py был запущен и бинарные файлы установлены.", file=sys.stderr)
                all_versions_ok = False
        
        if not all_versions_ok:
            print("\n✗ ОШИБКА: Не все версии приложений найдены!", file=sys.stderr)
            print("  Запустите create_apps.py для установки бинарных файлов и создания версий.", file=sys.stderr)
            return False
        
        # Определяем количество задач для каждого приложения
        tr = args.target_nresults
        counts_per_app = {}
        for app_name in targets:
            counts_per_app[app_name] = args.count if args.count is not None else APP_CONFIGS[app_name]["default_count"]
        
        # Выводим информацию о количестве задач для каждого приложения
        print("\nКоличество задач для каждого приложения:")
        for app_name in targets:
            print(f"  - {app_name}: {counts_per_app[app_name]} задач")
        
        # Определяем максимальное количество батчей (максимум из всех default_count)
        max_batches = max(counts_per_app.values())
        
        # Получаем список активных хостов для балансировки (если включена)
        hosts = None
        if args.balance_hosts:
            print("\nПолучение списка активных хостов для балансировки...")
            hosts = get_active_hosts()
            if hosts:
                print(f"  ✓ Найдено {len(hosts)} активных хостов:")
                for host in hosts[:10]:  # Показываем первые 10
                    print(f"    - Host {host['id']} ({host['domain_name']}): {host['task_count']} задач")
                if len(hosts) > 10:
                    print(f"    ... и еще {len(hosts) - 10} хостов")
            else:
                print("  ⚠ Хосты не найдены, задачи будут созданы без назначения хостов")
        else:
            print("\n⚠ Балансировка хостов отключена, задачи будут созданы без назначения хостов")
        
        # Создаем задачи батчами: по одной задаче каждого типа в каждом батче
        # Но только до достижения нужного количества для каждого приложения
        print(f"\nСоздание до {max_batches} батчей задач (по одной каждого типа в каждом батче)...")
        if args.balance_hosts:
            print("  ⚠ Балансировка хостов через --target_host отключена (не работает из-за ограничения BOINC)")
            print("  Используется обычный механизм scheduler с feeder flags для балансировки")
        all_created_tasks = []  # Список всех созданных задач: [(app_name, wu_name), ...]
        batch_threads = []
        batch_errors = []
        lock = threading.Lock()
        
        # Счетчики созданных задач для каждого приложения
        created_counts = {app_name: 0 for app_name in targets}
        
        def create_batch_thread(batch_num):
            try:
                # Получаем актуальный список хостов для каждого батча (для динамической балансировки)
                current_hosts = None
                if args.balance_hosts:
                    current_hosts = get_active_hosts()
                
                # Создаем задачи только для приложений, у которых еще не достигнуто нужное количество
                # Перемешиваем порядок приложений в каждом батче для случайного распределения
                app_names_for_batch = [app for app in targets if created_counts[app] < counts_per_app[app]]
                random.shuffle(app_names_for_batch)
                
                batch_tasks = []
                for app_name in app_names_for_batch:
                    if created_counts[app_name] < counts_per_app[app_name]:
                        # Используем уже подготовленные шаблоны
                        if app_name not in app_templates:
                            continue
                        
                        cfg = APP_CONFIGS[app_name]
                        tmpl_in_rel, tmpl_out_rel, placeholder_rel = app_templates[app_name]
                        
                        # Определяем хост для задачи (если есть хосты)
                        target_host_id = None
                        if current_hosts:
                            # Сортируем хосты по количеству задач (меньше задач = выше приоритет)
                            sorted_hosts = sorted(current_hosts, key=lambda x: x['task_count'])
                            # Используем round-robin для распределения
                            host_idx = (batch_num - 1) % len(sorted_hosts)
                            target_host_id = sorted_hosts[host_idx]['id']
                        
                        # Создаем одну задачу для этого приложения
                        task_list = create_workunits(
                            app_name, 1, tr, tmpl_in_rel, tmpl_out_rel, 
                            placeholder_rel, cfg["version_num"], 
                            start_index=batch_num, target_host_id=target_host_id
                        )
                        batch_tasks.extend(task_list)
                        with lock:
                            created_counts[app_name] += len(task_list)
                
                with lock:
                    all_created_tasks.extend(batch_tasks)
            except Exception as e:
                batch_errors.append((batch_num, str(e)))
                print(f"  ✗ Ошибка при создании батча {batch_num}: {e}", file=sys.stderr)
        
        # Запускаем батчи параллельно (но ограничиваем количество одновременных потоков)
        max_concurrent_batches = 10  # Максимум 10 батчей одновременно
        active_threads = []
        
        for batch_num in range(1, max_batches + 1):
            # Проверяем, нужно ли создавать еще батчи
            if all(created_counts[app_name] >= counts_per_app[app_name] for app_name in targets):
                print(f"  Все приложения достигли нужного количества задач, останавливаем создание батчей на батче {batch_num - 1}")
                break
            
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
                with lock:
                    progress_info = ", ".join([f"{app}: {created_counts[app]}/{counts_per_app[app]}" for app in targets])
                print(f"  Прогресс (батч {batch_num}): {progress_info}")
        
        # Ждем завершения всех оставшихся потоков
        for thread in active_threads:
            thread.join()
        
        # Ждем завершения всех потоков
        for thread in batch_threads:
            thread.join()
        
        if batch_errors:
            print(f"\n⚠ Обнаружены ошибки при создании батчей: {len(batch_errors)}", file=sys.stderr)
            for batch_num, error in batch_errors[:10]:  # Показываем первые 10 ошибок
                print(f"  - Батч {batch_num}: {error}", file=sys.stderr)
        
        # Выводим итоговую статистику
        print(f"\n✓ Создано {len(all_created_tasks)} задач. Ожидание синхронизации с БД...")
        with lock:
            for app_name in targets:
                print(f"  - {app_name}: {created_counts[app_name]}/{counts_per_app[app_name]} задач")
        time.sleep(2)  # Даем время БД синхронизироваться
        
        # Обновляем app_version_id для всех созданных задач одной транзакцией
        update_app_version_ids_batch(all_created_tasks)
        
        # Создаем host_app_version записи для всех хостов (для корректной работы scheduler)
        print("\nСоздание host_app_version записей для всех хостов...")
        create_host_app_versions_for_assigned(all_created_tasks)
        
        # Если использовалась балансировка хостов, создаем результаты вручную для назначенных задач
        # Это необходимо, т.к. назначенные задачи не создают результаты автоматически
        # из-за ограничения BOINC (work_needed(false) блокирует отправку)
        if args.balance_hosts:
            print("\nСоздание результатов для назначенных задач...")
            try:
                script_dir = os.path.dirname(os.path.abspath(__file__))
                create_results_script = os.path.join(script_dir, "create_results_simple.sh")
                # Копируем скрипт в контейнер и выполняем
                run_command("docker cp {} {}:/tmp/create_results_simple.sh".format(
                    create_results_script, CONTAINER_NAME
                ), check=False)
                result = run_command(
                    "docker exec {} bash /tmp/create_results_simple.sh".format(CONTAINER_NAME),
                    check=False
                )
                if result:
                    print("  ✓ Результаты созданы для назначенных задач")
                else:
                    print("  ⚠ Ошибка при создании результатов (проверьте логи)", file=sys.stderr)
            except Exception as e:
                print(f"  ⚠ Ошибка при создании результатов: {e}", file=sys.stderr)
    else:
        # Для одного приложения
        all_created_tasks = []
        for app_name in targets:
            cfg = APP_CONFIGS[app_name]
            count = args.count if args.count is not None else cfg["default_count"]
            tr = args.target_nresults
            created = process_app(app_name, count, tr)
            all_created_tasks.extend(created)
        
        # Обновляем app_version_id для всех созданных задач одной транзакцией
        if all_created_tasks:
            time.sleep(2)  # Даем время БД синхронизироваться
            update_app_version_ids_batch(all_created_tasks)
    
    # Перезапускаем feeder, чтобы он пересобрал массив задач с учетом --random_order_db
    # Это важно для равномерного распределения задач между клиентами
    print("\nОбновление feeder для пересборки массива задач...")
    run_command("touch {}/reread_db".format(PROJECT_HOME), check=False)  # Триггер для feeder на пересборку БД
    time.sleep(3)  # Даем время feeder пересобрать массив
    print("  ✓ Feeder обновлен (массив задач пересобран)")
    
    # Принудительно обновляем клиентов, чтобы они запросили работу
    if True:  # Всегда обновляем клиентов для быстрого получения задач
        print("\n" + "="*80)
        print("ВАЖНО: Для работы назначенных задач необходимо:")
        print("1. Убедиться, что scheduler перезапущен после включения debug_assignment")
        print("2. Проверить логи scheduler: tail -f logs/sched.log | grep -i assign")
        print("3. Если задачи не отправляются, проверьте, что клиенты запрашивают работу")
        print("="*80)
        print("\nПринудительное обновление клиентов для запроса назначенных задач...")
        try:
            # Запускаем update_clients.py как отдельный скрипт
            script_dir = os.path.dirname(os.path.abspath(__file__))
            update_clients_script = os.path.join(script_dir, "update_clients.py")
            
            # Запускаем скрипт через subprocess
            import subprocess
            result = subprocess.run(
                [sys.executable, update_clients_script],
                capture_output=True,
                text=True,
                timeout=120
            )
            if result.returncode == 0:
                print("  ✓ Клиенты обновлены, назначенные задачи будут отправлены при следующем RPC")
                print("\n  ⚠ ВНИМАНИЕ: Если задачи не отправляются, проверьте:")
                print("     - Логи scheduler на наличие ошибок 'App version for assigned WU not found'")
                print("     - Что scheduler перезапущен после изменений в config.xml")
                print("     - Что клиенты действительно запрашивают работу (проверьте логи клиентов)")
            else:
                print(f"  ⚠ Предупреждение: ошибка при обновлении клиентов: {result.stderr[:200]}", file=sys.stderr)
                print("  Клиенты автоматически запросят работу в течение 1 минуты")
        except Exception as e:
            print(f"  ⚠ Предупреждение: не удалось обновить клиентов: {e}", file=sys.stderr)
            print("  Клиенты автоматически запросят работу в течение 1 минуты")
    
    print("\nГотово. Проверьте в интерфейсе количество задач и готовность к отправке.")


if __name__ == "__main__":
    main()

