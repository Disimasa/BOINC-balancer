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

PROJECT_HOME = "/home/boincadm/project"
CONTAINER_NAME = "server-apache-1"

APP_CONFIGS = {
    "fast_task": {
        "script": "tasks/fast_task.py",
        "default_count": 10,
        "target_nresults": 2,
        "version_num": "100"
    },
    "medium_task": {
        "script": "tasks/medium_task.py",
        "default_count": 8,
        "target_nresults": 2,
        "version_num": "100"
    },
    "long_task": {
        "script": "tasks/long_task.py",
        "default_count": 5,
        "target_nresults": 2,
        "version_num": "100"
    },
    "random_task": {
        "script": "tasks/random_task.py",
        "default_count": 8,
        "target_nresults": 2,
        "version_num": "100"
    }
}


def run_command(cmd, check=True):
    """Выполнить команду в контейнере apache и вывести stdout/stderr."""
    # Устанавливаем переменные окружения для BOINC
    env_cmd = "export BOINC_PROJECT_DIR={proj} && cd {proj} && {cmd}".format(
        proj=PROJECT_HOME, cmd=cmd
    )
    full_cmd = [
        "wsl.exe", "-e", "docker", "exec", CONTAINER_NAME,
        "bash", "-lc", env_cmd
    ]
    print("Выполняю: {}".format(cmd))
    try:
        proc = subprocess.Popen(
            full_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        stdout, stderr = proc.communicate()
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

    cmd = (
        "mkdir -p {dest} && "
        "cp {bin} {dest}/{binary_name} && "
        "chmod +x {dest}/{binary_name} && "
        ": > {dest}/{binary_name}.sig"
    ).format(dest=platform_dir, bin=binary_path, binary_name=binary_name)
    if not run_command(cmd, check=True):
        return False, None, None

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
    cmd = "yes | bin/update_versions"
    run_command(cmd, check=False)


def create_workunits(app_name, count, target_nresults, tmpl_in_name, tmpl_out_name, placeholder_name, version_num):
    """Создать WU через bin/create_work."""
    import time
    # Используем timestamp для уникальности имён задач
    timestamp = int(time.time())
    
    for idx in range(1, count + 1):
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
        run_command(cmd, check=False)


def process_app(app_name, count, target_nresults):
    cfg = APP_CONFIGS[app_name]
    binary_path = os.path.join(PROJECT_HOME, "dist_bin", "{}_bin".format(app_name))
    ok_install, tmpl_in_rel, tmpl_out_rel, placeholder_rel = install_app_binary(app_name, binary_path, cfg["version_num"])
    if not ok_install or tmpl_in_rel is None or tmpl_out_rel is None:
        print("Не удалось установить бинарь для {}".format(app_name), file=sys.stderr)
        return
    register_app(app_name, friendly_name=app_name.replace("_", " ").title())
    create_workunits(app_name, count, target_nresults, tmpl_in_rel, tmpl_out_rel, placeholder_rel, cfg["version_num"])


def main():
    parser = argparse.ArgumentParser(description="Создание нативных задач из готовых бинарей (create_work)")
    parser.add_argument("--app", choices=list(APP_CONFIGS.keys()), help="Создать задачи только для одного приложения")
    parser.add_argument("--count", type=int, help="Количество задач (для одного app или для всех)")
    parser.add_argument("--target-nresults", type=int, default=2, help="Количество репликаций (по умолчанию 2)")
    args = parser.parse_args()

    ensure_download_hierarchy()

    targets = [args.app] if args.app else list(APP_CONFIGS.keys())

    for app_name in targets:
        cfg = APP_CONFIGS[app_name]
        count = args.count if args.count is not None else cfg["default_count"]
        tr = args.target_nresults
        process_app(app_name, count, tr)

    # Обновляем версии один раз в конце
    update_versions()

    print("\nГотово. Проверьте в интерфейсе количество задач и готовность к отправке.")


if __name__ == "__main__":
    main()

