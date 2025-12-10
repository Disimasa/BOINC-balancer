#!/bin/bash
# Скрипт для копирования .sig файлов и обновления версий

cd /home/boincadm/project

echo "Копирую .sig файлы..."

for app in fast_task medium_task long_task random_task; do
    for platform in x86_64-pc-linux-gnu__vbox64_mt windows_x86_64__vbox64_mt x86_64-apple-darwin__vbox64_mt; do
        src_dir="apps/boinc2docker/1.07/$platform"
        dst_dir="apps/$app/1.0/$platform"
        if [ -d "$src_dir" ] && [ -d "$dst_dir" ]; then
            # Копируем все .sig файлы
            cp "$src_dir"/*.sig "$dst_dir/" 2>/dev/null || true
        fi
    done
done

echo "Обновляю версии приложений..."
yes | bin/update_versions 2>&1 | head -100

echo "Готово!"


