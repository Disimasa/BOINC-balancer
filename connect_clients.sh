#!/bin/bash
# Скрипт для подключения клиентов BOINC к серверу

PROJECT_URL="${BOINC_PROJECT_URL:-http://apache/boincserver/}"

echo "Подключение клиентов к проекту: $PROJECT_URL"

for i in {1..20}; do
    container="boinc-client-$i"
    echo "Подключаю $container..."
    docker exec $container sh -c "PASSWD=\$(cat /var/lib/boinc/gui_rpc_auth.cfg 2>/dev/null || echo ''); boinccmd --passwd \"\$PASSWD\" --project_attach $PROJECT_URL ''" || echo "Ошибка подключения $container"
done

echo "Готово!"

