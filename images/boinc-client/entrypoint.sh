#!/bin/bash

# Исправление прав доступа для директорий BOINC
# Согласно документации: https://github.com/BOINC/boinc/wiki/Create-a-BOINC-server-(cookbook)
fix_permissions() {
    local dir=$1
    # Создаем директорию, если её нет
    mkdir -p "$dir" 2>/dev/null || true
    
    if [ -d "$dir" ]; then
        # Создаем необходимые поддиректории
        mkdir -p "$dir/slots" 2>/dev/null || true
        mkdir -p "$dir/projects" 2>/dev/null || true
        
        # Пробуем разные варианты пользователей
        chown -R boinc:boinc "$dir" 2>/dev/null || \
        chown -R boincadm:boincadm "$dir" 2>/dev/null || \
        chown -R $(id -u):$(id -g) "$dir" 2>/dev/null || true
        
        # Устанавливаем права на запись для всех
        chmod -R u+w "$dir" 2>/dev/null || true
        chmod -R 755 "$dir" 2>/dev/null || true
        
        # Исправляем права для всех поддиректорий projects (создаются динамически)
        if [ -d "$dir/projects" ]; then
            find "$dir/projects" -type d -exec chmod 755 {} \; 2>/dev/null || true
            find "$dir/projects" -type f -exec chmod 644 {} \; 2>/dev/null || true
            chown -R boinc:boinc "$dir/projects" 2>/dev/null || \
            chown -R boincadm:boincadm "$dir/projects" 2>/dev/null || \
            chown -R $(id -u):$(id -g) "$dir/projects" 2>/dev/null || true
        fi
    fi
}

# Исправляем права для обеих возможных директорий
fix_permissions /var/lib/boinc
fix_permissions /var/lib/boinc-client

# Запускаем стандартную команду образа
# Если аргументы не переданы, используем стандартную команду образа
if [ $# -eq 0 ]; then
    exec start-boinc.sh
else
    exec "$@"
fi

