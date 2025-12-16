#!/usr/bin/env python3
import subprocess
import sys
import argparse
import os
import threading
import time
import random
from pathlib import Path
from tqdm import tqdm
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from lib.utils import run_command as run_cmd, check_file_exists, run_local_command, PROJECT_HOME, CONTAINER_NAME, SCRIPT_DIR

def run_command(cmd, check=True, capture_output=False):
    return run_cmd(f"cd {PROJECT_HOME} && {cmd}", check=check, capture_output=capture_output)

APP_CONFIGS = {
    "fast_task": {
        "script": "tasks/fast_task.py",
        "default_count": 20000,
        "target_nresults": 1,
        "version_num": "100"
    },
    "medium_task": {
        "script": "tasks/medium_task.py",
        "default_count": 8000,
        "target_nresults": 1,
        "version_num": "100"
    },
    "random_task": {
        "script": "tasks/random_task.py",
        "default_count": 8000,
        "target_nresults": 1,
        "version_num": "100"
    },
    "long_task": {
        "script": "tasks/long_task.py",
        "default_count": 2000,
        "target_nresults": 1,
        "version_num": "100"
    }
}






def copy_file_to_container(host_path, container_path):
    if not os.path.exists(host_path):
        print(f"Файл на хосте не найден: {host_path}", file=sys.stderr)
        return False
    try:
        result = run_local_command(
            ["docker", "cp", host_path, f"{CONTAINER_NAME}:{container_path}"],
            check=False,
            capture_output=True
        )
        if result.returncode != 0:
            stderr_msg = result.stderr if result.stderr else ""
            print(f"Ошибка при копировании {host_path} в контейнер: {stderr_msg}", file=sys.stderr)
            return False
        return True
    except Exception as e:
        print(f"Ошибка при копировании {host_path} в контейнер: {e}", file=sys.stderr)
        return False


def ensure_templates_and_placeholder(app_name):
    script_dir = SCRIPT_DIR
    host_tmpl_in = os.path.join(script_dir, "templates", f"{app_name}_in.xml")
    host_tmpl_out = os.path.join(script_dir, "templates", f"{app_name}_out.xml")
    
    container_tmpl_in = f"{PROJECT_HOME}/templates/{app_name}_in.xml"
    container_tmpl_out = f"{PROJECT_HOME}/templates/{app_name}_out.xml"
    
    if not check_file_exists(container_tmpl_in):
        if not copy_file_to_container(host_tmpl_in, container_tmpl_in):
            return False
    
    if not check_file_exists(container_tmpl_out):
        if not copy_file_to_container(host_tmpl_out, container_tmpl_out):
            return False
    
    return True


def install_app_binary(app_name, binary_path, version_num):
    platform_dir = f"apps/{app_name}/1.0/x86_64-pc-linux-gnu"
    binary_name = f"{app_name}_bin"
    
    if not ensure_templates_and_placeholder(app_name):
        print(f"Не удалось скопировать шаблоны для {app_name}", file=sys.stderr)
        return False, None, None, None
    
    tmpl_in_rel = f"templates/{app_name}_in.xml"
    tmpl_out_rel = f"templates/{app_name}_out.xml"
    placeholder_rel = None
    tmpl_in_path = f"{PROJECT_HOME}/{tmpl_in_rel}"
    tmpl_out_path = f"{PROJECT_HOME}/{tmpl_out_rel}"
    
    missing = []
    check_files = [
        (binary_path, "бинарный файл"),
        (tmpl_in_path, "шаблон ввода"),
        (tmpl_out_path, "шаблон вывода")
    ]
    
    for p, desc in check_files:
        if not check_file_exists(p):
            missing.append((p, desc))
    if missing:
        for p, desc in missing:
            print(f"Отсутствует файл ({desc}): {p}", file=sys.stderr)
        return False, None, None, None

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
        "  bin/sign_executable \"$bin_path\" \"$key1\" > \"$sig_path\" 2>&1; "
        "elif [ -f \"$key2\" ] && [ ! -c \"$key2\" ] && [ -s \"$key2\" ]; then "
        "  bin/sign_executable \"$bin_path\" \"$key2\" > \"$sig_path\" 2>&1; "
        "else "
        "  echo 'Warning: code_sign_private key not found, update_versions will try to sign it'; "
        "  rm -f \"$sig_path\"; "
        "fi"
    ).format(
        bin_path=binary_full_path,
        sig_path=sig_path
    )
    sign_result, success = run_command(cmd_sign, check=False, capture_output=True)
    if not success:
        run_command(f"rm -f {sig_path}", check=False)
        print(f"⚠ Предупреждение: не удалось подписать {binary_name}. "
              f"update_versions попытается подписать его автоматически.", file=sys.stderr)

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

    placeholder_name = os.path.basename(placeholder_rel) if placeholder_rel else None
    tmpl_in_name = os.path.basename(tmpl_in_rel)
    tmpl_out_name = os.path.basename(tmpl_out_rel)
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
    
    if placeholder_name:
        cmd_stage = f"bin/stage_file --copy --verbose {placeholder_rel}"
        if not run_command(cmd_stage, check=False):
            print(f"  ⚠ Предупреждение: stage_file вернул ошибку для {placeholder_rel}, но продолжаем", file=sys.stderr)
            check_cmd = "find download -name 'input_placeholder' -type f 2>/dev/null | head -1"
            check_result = run_command(check_cmd, check=False, capture_output=True)
            if not check_result:
                print(f"  ✗ Файл {placeholder_rel} не найден в download/", file=sys.stderr)
                return False, None, None, None
    else:
        print("  ℹ Входные файлы не требуются для этого типа задач")

    return True, tmpl_in_base, tmpl_out_base, placeholder_name


def ensure_download_hierarchy():
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
    cmd = "yes | bin/update_versions 2>&1"
    return run_command(cmd, check=False)


def register_app(app_name, friendly_name=None):
    title = friendly_name or app_name
    cmd = "bin/xadd {app} \"{title}\" \"\"".format(app=app_name, title=title)
    run_command(cmd, check=False)  # допускаем, что уже существует


def get_app_version_id(app_name, version_num):
    if isinstance(version_num, str):
        try:
            version_num_int = int(version_num)
        except ValueError:
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
    output, success = run_command(cmd, capture_output=True)
    if output and isinstance(output, str) and output.strip().isdigit():
        return int(output.strip())
    return None


def ensure_app_version_exists(app_name, version_num):
    app_version_id = get_app_version_id(app_name, version_num)
    if app_version_id is None:
        print(f"✗ ОШИБКА: app_version не найден для {app_name} версии {version_num}!", file=sys.stderr)
        print(f"  Это означает, что update_versions не был запущен или версия не была создана.", file=sys.stderr)
        print(f"  Задачи будут созданы с app_version_id=0 и не будут отправляться клиентам.", file=sys.stderr)
        return False
    return True


def create_workunits(app_name, count, target_nresults, tmpl_in_name, tmpl_out_name, placeholder_name, version_num, start_index=1, target_host_id=None):
    import time
    timestamp = int(time.time())
    
    created_wu_names = []
    
    for idx in range(start_index, start_index + count):
        wu_name = "{app}_native_{ts}_{idx}".format(app=app_name, ts=timestamp, idx=idx)
        
        target_host_param = ""
        if target_host_id is not None:
            target_host_param = f"--target_host {target_host_id}"
        placeholder_param = ""
        if placeholder_name:
            placeholder_param = placeholder_name
        
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
        result = run_local_command(
            ["docker", "exec", "-i", CONTAINER_NAME,
                "bash", "-c", "cd {} && mysql -u root -ppassword boincserver".format(PROJECT_HOME)],
            input=sql_content,
            capture_output=True,
            check=False
        )
        stdout = result.stdout if result.stdout else ""
        stderr = result.stderr if result.stderr else ""
        
        result = proc.returncode == 0
        if stderr and not result:
            print(f"⚠ SQL ошибка: {stderr[:200]}", file=sys.stderr)
    except Exception as e:
        print(f"⚠ Ошибка при выполнении SQL: {e}", file=sys.stderr)
        result = False
    
    if result:
        total_updated = sum(len(wu_names) for wu_names in tasks_by_app.values())
        
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
                result = run_local_command(
                    ["docker", "exec", "-i", CONTAINER_NAME,
                        "bash", "-c", "cd {} && mysql -u root -ppassword boincserver -N".format(PROJECT_HOME)],
                    input=check_sql,
                    capture_output=True,
                    check=False
                )
                check_output = result.stdout if result.stdout else ""
            except Exception:
                check_output = None
            if check_output and check_output.strip().isdigit():
                count = int(check_output.strip())
                if count != len(wu_names):
                    print(f"⚠ {app_name}: только {count} из {len(wu_names)} задач имеют app_version_id = {app_version_id}", file=sys.stderr)
    else:
        print("⚠ Ошибка при обновлении app_version_id", file=sys.stderr)


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
        return True
    else:
        print(f"✗ ОШИБКА: Ключ подписи не найден или пустой: {key_path}", file=sys.stderr)
        print(f"  Ключ должен быть создан в pipeline.py на этапе генерации ключей.", file=sys.stderr)
        print(f"  Запустите pipeline.py заново или создайте ключ вручную: python3 generate_keys.py", file=sys.stderr)
        return False


def create_tasks(app=None, count=None, target_nresults=1, balance_hosts=False):
    ensure_download_hierarchy()
    
    targets = [app] if app else list(APP_CONFIGS.keys())

    if not app and len(targets) > 1:
        app_templates = {}
        for app_name in targets:
            if not ensure_templates_and_placeholder(app_name):
                print(f"✗ {app_name}: ошибка подготовки шаблонов", file=sys.stderr)
                continue
            
            tmpl_in_rel = f"templates/{app_name}_in.xml"
            tmpl_out_rel = f"templates/{app_name}_out.xml"
            placeholder_rel = None
            
            tmpl_in_base = os.path.basename(tmpl_in_rel).replace('.xml', '')
            tmpl_out_base = os.path.basename(tmpl_out_rel).replace('.xml', '')
            cmd_create_templates = f"cp templates/{os.path.basename(tmpl_in_rel)} templates/{tmpl_in_base} && cp templates/{os.path.basename(tmpl_out_rel)} templates/{tmpl_out_base}"
            if run_command(cmd_create_templates, check=False):
                placeholder_name = None
                app_templates[app_name] = (tmpl_in_base, tmpl_out_base, placeholder_name)
            else:
                print(f"✗ {app_name}: ошибка создания шаблонов", file=sys.stderr)
        
        if len(app_templates) != len(targets):
            print("✗ Не удалось подготовить шаблоны для всех приложений", file=sys.stderr)
            return False
        all_versions_ok = True
        for app_name in targets:
            app_version_id = get_app_version_id(app_name, APP_CONFIGS[app_name]["version_num"])
            if not app_version_id:
                print(f"✗ {app_name}: версия не найдена в БД!", file=sys.stderr)
                all_versions_ok = False
        
        if not all_versions_ok:
            print("\n✗ ОШИБКА: Не все версии приложений найдены!", file=sys.stderr)
            return False
        
        if len(app_templates) != len(targets):
            print("✗ Не удалось подготовить шаблоны для всех приложений", file=sys.stderr)
            return False
        
        tr = target_nresults
        counts_per_app = {}
        for app_name in targets:
            counts_per_app[app_name] = count if count is not None else APP_CONFIGS[app_name]["default_count"]
        
        print("\nКоличество задач для каждого приложения:")
        for app_name in targets:
            print(f"  - {app_name}: {counts_per_app[app_name]} задач")
        
        max_batches = max(counts_per_app.values())
        
        all_created_tasks = []
        batch_threads = []
        batch_errors = []
        lock = threading.Lock()
        
        created_counts = {app_name: 0 for app_name in targets}
        
        def create_batch_thread(batch_num, progress_bar):
            try:
                current_hosts = None
                if balance_hosts:
                    current_hosts = get_active_hosts()
                
                app_names_for_batch = [app for app in targets if created_counts[app] < counts_per_app[app]]
                random.shuffle(app_names_for_batch)
                
                batch_tasks = []
                for app_name in app_names_for_batch:
                    if created_counts[app_name] < counts_per_app[app_name]:
                        if app_name not in app_templates:
                            continue
                        
                        cfg = APP_CONFIGS[app_name]
                        tmpl_in_rel, tmpl_out_rel, placeholder_rel = app_templates[app_name]
                        
                        target_host_id = None
                        if current_hosts:
                            sorted_hosts = sorted(current_hosts, key=lambda x: x['task_count'])
                            host_idx = (batch_num - 1) % len(sorted_hosts)
                            target_host_id = sorted_hosts[host_idx]['id']
                        
                        task_list = create_workunits(
                            app_name, 1, tr, tmpl_in_rel, tmpl_out_rel, 
                            placeholder_rel, cfg["version_num"], 
                            start_index=batch_num, target_host_id=target_host_id
                        )
                        batch_tasks.extend(task_list)
                        with lock:
                            created_counts[app_name] += len(task_list)
                            progress_bar.update(len(task_list))
                
                with lock:
                    all_created_tasks.extend(batch_tasks)
            except Exception as e:
                batch_errors.append((batch_num, str(e)))
                print(f"  ✗ Ошибка при создании батча {batch_num}: {e}", file=sys.stderr)
        
        max_concurrent_batches = 10
        active_threads = []
        total_tasks = sum(counts_per_app.values())
        
        with tqdm(total=total_tasks, desc="Создание задач", unit="задача") as pbar:
            for batch_num in range(1, max_batches + 1):
                if all(created_counts[app_name] >= counts_per_app[app_name] for app_name in targets):
                    break
                
                thread = threading.Thread(target=create_batch_thread, args=(batch_num, pbar))
                thread.start()
                active_threads.append(thread)
                batch_threads.append(thread)
                
                if len(active_threads) >= max_concurrent_batches:
                    active_threads[0].join()
                    active_threads.pop(0)
            
            for thread in active_threads:
                thread.join()
            
            for thread in batch_threads:
                thread.join()
        
        if batch_errors:
            print(f"\n⚠ Обнаружены ошибки при создании батчей: {len(batch_errors)}", file=sys.stderr)
            for batch_num, error in batch_errors[:10]:
                print(f"  - Батч {batch_num}: {error}", file=sys.stderr)
        
        print(f"\n✓ Создано {len(all_created_tasks)} задач. Ожидание синхронизации с БД...")
        with lock:
            for app_name in targets:
                print(f"  - {app_name}: {created_counts[app_name]}/{counts_per_app[app_name]} задач")
        # time.sleep(2)
        #
        # update_app_version_ids_batch(all_created_tasks)
    else:
        all_created_tasks = []
        total_tasks = sum(count if count is not None else APP_CONFIGS[app_name]["default_count"] for app_name in targets)
        with tqdm(total=total_tasks, desc="Создание задач", unit="задача") as pbar:
            for app_name in targets:
                cfg = APP_CONFIGS[app_name]
                app_count = count if count is not None else cfg["default_count"]
                tr = target_nresults
                created = process_app(app_name, app_count, tr)
                all_created_tasks.extend(created)
                pbar.update(len(created))
        
        # if all_created_tasks:
        #     time.sleep(2)
        #     update_app_version_ids_batch(all_created_tasks)
        
        # print("\nОбновление feeder для пересборки массива задач...")
        # run_command(f"touch {PROJECT_HOME}/reread_db", check=False)
        # time.sleep(3)
        # print("  ✓ Feeder обновлен")
    
    return True


def main():
    parser = argparse.ArgumentParser(description="Создание нативных задач из готовых бинарей (create_work)")
    parser.add_argument("--app", choices=list(APP_CONFIGS.keys()), help="Создать задачи только для одного приложения")
    parser.add_argument("--count", type=int, help="Количество задач (для одного app или для всех)")
    parser.add_argument("--target-nresults", type=int, default=1, help="Количество репликаций (по умолчанию 1)")
    parser.add_argument("--balance-hosts", action="store_true", help="Балансировать задачи между хостами (по умолчанию отключено)")
    args = parser.parse_args()
    
    return create_tasks(
        app=args.app,
        count=args.count,
        target_nresults=args.target_nresults,
        balance_hosts=args.balance_hosts
    )


if __name__ == "__main__":
    main()

