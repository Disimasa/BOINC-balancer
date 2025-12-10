#!/bin/bash
# Скрипт для принудительного обновления версий всех приложений

cd /home/boincadm/project

echo "Принудительно обновляю версии для всех приложений..."

# Список приложений
apps=("fast_task" "medium_task" "long_task" "random_task")

# Для каждого приложения проверяем наличие версий и обновляем
for app in "${apps[@]}"; do
    echo ""
    echo "=== Processing $app ==="
    
    # Проверяем наличие директорий версий
    if [ -d "apps/$app/1.0" ]; then
        echo "Found app directory: apps/$app/1.0"
        
        # Проверяем наличие платформ
        for platform_dir in apps/$app/1.0/*/; do
            if [ -d "$platform_dir" ]; then
                platform=$(basename "$platform_dir")
                echo "  Found platform: $platform"
                
                # Проверяем наличие необходимых файлов
                if [ -f "$platform_dir/version.xml" ] && [ -f "$platform_dir/vm_isocontext_v1.0.0.iso" ]; then
                    echo "    Files OK: version.xml and vm_isocontext_v1.0.0.iso"
                else
                    echo "    WARNING: Missing files in $platform_dir"
                fi
            fi
        done
    else
        echo "ERROR: App directory not found: apps/$app/1.0"
    fi
done

echo ""
echo "Запускаю update_versions..."
# Запускаем update_versions несколько раз, чтобы обработать все приложения
for i in {1..5}; do
    echo "Attempt $i..."
    yes | bin/update_versions 2>&1 | grep -E "Found app version directory|already exists in database" | head -20
    sleep 1
done

echo ""
echo "Готово! Проверьте веб-интерфейс."


