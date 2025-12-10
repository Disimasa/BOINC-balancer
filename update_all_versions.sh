#!/bin/bash
# Скрипт для обновления версий всех приложений

cd /home/boincadm/project

echo "Обновляю версии всех приложений..."

for app in fast_task medium_task long_task random_task; do
    echo "=== Processing $app ==="
    # Проверяем, что директории существуют
    if [ -d "apps/$app/1.0" ]; then
        echo "Found app: $app"
        # Обновляем версии для этого приложения
        yes | bin/update_versions 2>&1 | grep -E "Found|Error|already exists" | head -10
    else
        echo "App $app not found in apps/"
    fi
done

echo "Готово!"


