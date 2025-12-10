#!/usr/bin/env python3
"""Упрощенный скрипт для создания BOINC приложений без yaml"""
import subprocess
import sys
import os

CONTAINER_NAME = "server-apache-1"
PROJECT_HOME = "/home/boincadm/project"

# Конфигурация приложений
apps = [
    {"name": "fast_task", "resultsdir": "/results/fast_task"},
    {"name": "medium_task", "resultsdir": "/results/medium_task"},
    {"name": "long_task", "resultsdir": "/results/long_task"},
    {"name": "random_task", "resultsdir": "/results/random_task"},
]

def run_command(cmd):
    """Выполнить команду в контейнере apache"""
    if os.getenv("RUNNING_IN_DOCKER"):
        full_cmd = cmd
    else:
        full_cmd = ["wsl.exe", "-e", "docker", "exec", CONTAINER_NAME, "bash", "-c", cmd]
    
    print(f"Выполняю: {cmd}")
    try:
        result = subprocess.run(
            full_cmd if not os.getenv("RUNNING_IN_DOCKER") else cmd,
            shell=bool(os.getenv("RUNNING_IN_DOCKER")),
            capture_output=True,
            text=True,
            timeout=300
        )
        if result.stdout:
            print(result.stdout)
        if result.stderr and result.returncode != 0:
            print(f"Ошибка: {result.stderr}", file=sys.stderr)
        return result.returncode == 0
    except Exception as e:
        print(f"Ошибка выполнения команды: {e}", file=sys.stderr)
        return False

def create_app(app_name, resultsdir):
    """Создать приложение, копируя структуру из boinc2docker"""
    print(f"\nСоздаю App: {app_name}")
    
    # Копируем структуру из boinc2docker
    cmd = f"""
    cd {PROJECT_HOME} && \
    mkdir -p apps/{app_name}/1.0 && \
    for platform in x86_64-pc-linux-gnu__vbox64_mt windows_x86_64__vbox64_mt x86_64-apple-darwin__vbox64_mt; do \
        mkdir -p apps/{app_name}/1.0/$platform && \
        cp -r apps/boinc2docker/1.07/$platform/* apps/{app_name}/1.0/$platform/ 2>/dev/null || true; \
    done
    """
    
    if not run_command(cmd):
        print(f"Ошибка при копировании структуры для {app_name}", file=sys.stderr)
        return False
    
    # Создаем assimilator
    cmd = f"""
    cd {PROJECT_HOME} && \
    cat > bin/{app_name}_assimilator << 'EOF'
#!/bin/bash
# Assimilator for {app_name}
RESULTS_DIR={resultsdir}
mkdir -p "$RESULTS_DIR"
# Копируем результаты
for file in "$@"; do
    if [ -f "$file" ]; then
        cp "$file" "$RESULTS_DIR/"
    fi
done
EOF
    chmod +x bin/{app_name}_assimilator
    """
    
    if not run_command(cmd):
        print(f"Ошибка при создании assimilator для {app_name}", file=sys.stderr)
        return False
    
    # Обновляем config.xml, project.xml, plan_class_spec.xml
    # Добавляем daemon в config.xml
    cmd = f"""
    cd {PROJECT_HOME} && \
    if ! grep -q '{app_name}_assimilator' config.xml; then \
        sed -i '/<\\/daemons>/i\\        <daemon>\\n            <cmd>script_assimilator --app {app_name} --script "{app_name}_assimilator files"</cmd>\\n        </daemon>' config.xml; \
    fi
    """
    run_command(cmd)
    
    # Добавляем app в project.xml
    cmd = f"""
    cd {PROJECT_HOME} && \
    if ! grep -q '<name>{app_name}</name>' project.xml; then \
        sed -i '/<\\/boinc>/i\\    <app>\\n        <name>{app_name}</name>\\n        <user_friendly_name>{app_name}</user_friendly_name>\\n    </app>\\n' project.xml; \
    fi
    """
    run_command(cmd)
    
    print(f"Приложение {app_name} создано и настроено")
    return True

if __name__ == "__main__":
    print("=" * 60)
    print("Создание Apps...")
    print("=" * 60)
    
    for app in apps:
        create_app(app["name"], app["resultsdir"])
    
    print("\n" + "=" * 60)
    print("Регистрирую Apps в базе данных...")
    print("=" * 60)
    run_command(f"cd {PROJECT_HOME} && bin/xadd")
    
    print("\n" + "=" * 60)
    print("Обновляю версии Apps...")
    print("=" * 60)
    run_command(f"cd {PROJECT_HOME} && yes | bin/update_versions")
    
    print("\n" + "=" * 60)
    print("Все Apps успешно созданы и зарегистрированы!")
    print("=" * 60)
    print("\nПримечание: Если приложения не видны в веб-интерфейсе,")
    print("попробуйте перезапустить демоны BOINC: bin/stop && bin/start")

