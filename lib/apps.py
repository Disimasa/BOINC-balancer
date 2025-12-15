#!/usr/bin/env python3
import os
import sys
from lib.utils import run_command, check_file_exists, PROJECT_HOME, CONTAINER_NAME

APPS = [
    {"name": "fast_task", "resultsdir": "/results/fast_task", "weight": 1.0},
    {"name": "medium_task", "resultsdir": "/results/medium_task", "weight": 1.0},
    {"name": "random_task", "resultsdir": "/results/random_task", "weight": 1.0},
    {"name": "long_task", "resultsdir": "/results/long_task", "weight": 1.0}
]


def create_app(app_name, resultsdir, weight=1.0):
    run_cmd = lambda cmd, check=True: run_command(f"cd {PROJECT_HOME} && {cmd}", check=check)
    
    run_cmd(f"mkdir -p apps/{app_name}/1.0/x86_64-pc-linux-gnu", check=False)
    
    run_cmd(f"""cat > bin/{app_name}_assimilator << 'EOF'
#!/bin/bash
RESULTS_DIR={resultsdir}
mkdir -p "$RESULTS_DIR"
for file in "$@"; do
    if [ -f "$file" ]; then
        mv "$file" "$RESULTS_DIR/" 2>/dev/null || cp "$file" "$RESULTS_DIR/"
    fi
done
EOF
chmod +x bin/{app_name}_assimilator && chown boincadm:boincadm bin/{app_name}_assimilator""", check=False)
    
    run_cmd(f"""if ! grep -q 'sample_trivial_validator.*{app_name}' config.xml; then
        sed -i '/<\\/daemons>/i\\        <daemon>\\n            <cmd>sample_trivial_validator -app {app_name}</cmd>\\n        </daemon>' config.xml
    fi""", check=False)
    
    run_cmd(f"""if ! grep -q '{app_name}_assimilator' config.xml; then
        sed -i '/<\\/daemons>/i\\        <daemon>\\n            <cmd>script_assimilator --app {app_name} --script "{app_name}_assimilator files" --sleep_interval 30</cmd>\\n        </daemon>' config.xml
    else
        sed -i 's|script_assimilator --app {app_name} --script "{app_name}_assimilator files"|script_assimilator --app {app_name} --script "{app_name}_assimilator files" --sleep_interval 30|g' config.xml
    fi""", check=False)
    
    run_cmd(f"""if ! grep -q '<name>{app_name}</name>' project.xml; then
        sed -i '/<\\/boinc>/i\\    <app>\\n        <name>{app_name}</name>\\n        <user_friendly_name>{app_name}</user_friendly_name>\\n    </app>\\n' project.xml
    fi""", check=False)
    
    run_cmd("bin/xadd > /dev/null 2>&1", check=False)
    
    run_cmd(f"""mysql -u root -ppassword boincserver -e "UPDATE app SET weight = {weight} WHERE name = '{app_name}';" """, check=False)
    return True


def setup_daemons():
    apps_list = " ".join([app['name'] for app in APPS])
    run_cmd = lambda cmd, check=True: run_command(f"cd {PROJECT_HOME} && {cmd}", check=check)
    
    run_cmd(f"""mkdir -p ../bin && for app in {apps_list}; do
        ln -sf {PROJECT_HOME}/bin/${{app}}_assimilator ../bin/${{app}}_assimilator
    done""", check=False)
    
    results_dirs = " ".join([app['resultsdir'] for app in APPS])
    run_command(f"mkdir -p {results_dirs} && chown -R boincadm:boincadm /results && chmod -R 755 /results", check=False)
    
    run_cmd("""sed -i 's|<cmd>feeder -d 3[^<]*</cmd>|<cmd>feeder -d 3 --allapps --priority_order --sleep_interval 1</cmd>|g' config.xml""", check=False)
    
    run_cmd("""if ! grep -q '<max_wus_to_send>' config.xml; then
        sed -i '/<\\/boinc>/i\\    <max_wus_to_send>1</max_wus_to_send>' config.xml
    else
        sed -i 's|<max_wus_to_send>[0-9]*</max_wus_to_send>|<max_wus_to_send>1</max_wus_to_send>|g' config.xml
    fi""", check=False)
    run_cmd("""if ! grep -q '<max_wus_in_progress>' config.xml; then
        sed -i '/<\\/boinc>/i\\    <max_wus_in_progress>1</max_wus_in_progress>' config.xml
    else
        sed -i 's|<max_wus_in_progress>[0-9]*</max_wus_in_progress>|<max_wus_in_progress>1</max_wus_in_progress>|g' config.xml
    fi""", check=False)
    run_cmd("""if ! grep -q '<max_ncpus>' config.xml; then
        sed -i '/<\\/boinc>/i\\    <max_ncpus>1</max_ncpus>' config.xml
    else
        sed -i 's|<max_ncpus>[0-9]*</max_ncpus>|<max_ncpus>1</max_ncpus>|g' config.xml
    fi""", check=False)
    
    run_cmd("""if ! grep -q '<min_sendwork_interval>' config.xml; then
        sed -i '/<\\/boinc>/i\\    <min_sendwork_interval>2</min_sendwork_interval>' config.xml
    else
        sed -i 's|<min_sendwork_interval>[0-9]*</min_sendwork_interval>|<min_sendwork_interval>2</min_sendwork_interval>|g' config.xml
    fi""", check=False)
    
    run_cmd("""if ! grep -q '<enable_assignment>' config.xml; then
        sed -i '/<\\/boinc>/i\\    <enable_assignment/>' config.xml
    fi""", check=False)
    
    run_cmd("""if ! grep -q '<debug_assignment>' config.xml; then
        sed -i '/<\\/boinc>/i\\    <debug_assignment/>' config.xml
    fi""", check=False)
    
    run_cmd("""if ! grep -q '<debug_send>' config.xml; then
        sed -i '/<\\/boinc>/i\\    <debug_send/>' config.xml
    fi""", check=False)
    
    run_cmd("bin/stop && sleep 2 && bin/start", check=False)


def install_app_binary(app_name, binary_path, version_num="100"):
    platform_dir = f"apps/{app_name}/1.0/x86_64-pc-linux-gnu"
    binary_name = f"{app_name}_bin"
    
    if not check_file_exists(binary_path):
        print(f"  ⚠ Предупреждение: бинарный файл не найден: {binary_path}", file=sys.stderr)
        return False
    
    run_cmd = lambda cmd, check=True: run_command(f"cd {PROJECT_HOME} && {cmd}", check=check)
    
    cmd = f"mkdir -p {platform_dir} && cp {binary_path} {platform_dir}/{binary_name} && chmod +x {platform_dir}/{binary_name}"
    if not run_cmd(cmd, check=False):
        print(f"  ✗ Ошибка при копировании бинарного файла для {app_name}", file=sys.stderr)
        return False
    
    binary_full_path = os.path.join(platform_dir, binary_name)
    sig_path = os.path.join(platform_dir, f"{binary_name}.sig")
    
    cmd_sign = f"""bin_path='{binary_full_path}' && sig_path='{sig_path}' && key1='keys/code_sign_private' && key2='/run/secrets/keys/code_sign_private' && if [ -f "$key1" ] && [ ! -c "$key1" ] && [ -s "$key1" ]; then bin/sign_executable "$bin_path" "$key1" > "$sig_path" 2>&1 && echo 'Signed with keys/code_sign_private'; elif [ -f "$key2" ] && [ ! -c "$key2" ] && [ -s "$key2" ]; then bin/sign_executable "$bin_path" "$key2" > "$sig_path" 2>&1 && echo 'Signed with /run/secrets/keys/code_sign_private'; else echo 'Warning: code_sign_private key not found'; rm -f "$sig_path"; fi"""
    run_cmd(cmd_sign, check=False)
    
    version_xml = f"""cat > {platform_dir}/version.xml <<EOF
<version>
  <app_name>{app_name}</app_name>
  <version_num>{version_num}</version_num>
  <platform>x86_64-pc-linux-gnu</platform>
  <file_ref>
    <file_name>{binary_name}</file_name>
    <main_program/>
  </file_ref>
</version>
EOF"""
    run_cmd(version_xml, check=False)
    return True


def update_versions():
    run_cmd = lambda cmd, check=True: run_command(f"cd {PROJECT_HOME} && {cmd}", check=check)
    run_cmd("yes | bin/update_versions > /dev/null 2>&1", check=False)
    
    import subprocess
    for app in APPS:
        app_name = app['name']
        version_num = 100
        query = f"SELECT av.id FROM app_version av JOIN app a ON av.appid = a.id WHERE a.name = '{app_name}' AND av.version_num = {version_num} AND av.deprecated = 0 LIMIT 1"
        check_cmd = f"cd {PROJECT_HOME} && mysql -u root -ppassword boincserver -N -e \"{query}\""
        stdout, success = run_command(check_cmd, check=False, capture_output=True)
        if not (stdout and stdout.strip().isdigit()):
            print(f"✗ {app_name}: версия не найдена в БД!", file=sys.stderr)


def get_current_weights():
    query = """
    SELECT name, weight 
    FROM app 
    WHERE name IN ('fast_task', 'medium_task', 'long_task', 'random_task') 
        AND deprecated = 0
    ORDER BY name;
    """
    
    cmd = f"cd {PROJECT_HOME} && mysql -u root -ppassword boincserver -N -e \"{query}\""
    stdout, success = run_command(cmd, check=False, capture_output=False)
    print(stdout)
    if not success or not stdout:
        return {}
    
    weights = {}
    for line in stdout.strip().split('\n'):
        if not line.strip():
            continue
        parts = line.split('\t')
        if len(parts) >= 2:
            app_name = parts[0].strip()
            try:
                weight = float(parts[1].strip())
                weights[app_name] = weight
            except (ValueError, IndexError):
                continue
    
    return weights


def update_weights(new_weights):
    """Обновить веса приложений в БД."""
    if not new_weights:
        return False
    
    update_statements = []
    for app_name, weight in new_weights.items():
        update_statements.append(
            "UPDATE app SET weight = {} WHERE name = '{}';".format(weight, app_name)
        )
    
    sql = "START TRANSACTION;\n" + "\n".join(update_statements) + "\nCOMMIT;"
    
    mysql_cmd = f"""bash -c "mysql -u root -ppassword boincserver << 'EOFSQL'
{sql}
EOFSQL" """
    
    _, success = run_command(f"cd {PROJECT_HOME} && {mysql_cmd}", check=False)
    return success


def create_all_apps():
    for app in APPS:
        weight = app.get('weight', 1.0)
        create_app(app['name'], app['resultsdir'], weight)
    
    setup_daemons()
    
    run_cmd = lambda cmd, check=True: run_command(f"cd {PROJECT_HOME} && {cmd}", check=check)
    for app in APPS:
        run_cmd(f"cd templates && [ ! -f {app['name']}_out ] && cp boinc2docker_out {app['name']}_out", check=False)
    
    binaries_installed = True
    for app in APPS:
        app_name = app['name']
        binary_path = os.path.join(PROJECT_HOME, "dist_bin", f"{app_name}_bin")
        if not install_app_binary(app_name, binary_path, "100"):
            binaries_installed = False
    
    if binaries_installed:
        update_versions()
    else:
        print("⚠ Предупреждение: не все бинарные файлы были установлены.", file=sys.stderr)

