#!/bin/bash
set -e

# Создаем необходимые директории и устанавливаем права
mkdir -p /var/lib/boinc/slots /var/lib/boinc/projects
mkdir -p /var/lib/boinc/projects/172.26.176.1_boincserver
mkdir -p /var/lib/boinc/projects/boincserver_boincserver
mkdir -p /var/lib/boinc/projects/localhost_boincserver

# Принудительно устанавливаем права на все директории (включая существующие в volume)
chmod -R 755 /var/lib/boinc
chown -R boinc:boinc /var/lib/boinc || {
    # Если chown не работает, пробуем через find
    find /var/lib/boinc -type d -exec chmod 755 {} \;
    find /var/lib/boinc -type f -exec chmod 644 {} \;
    find /var/lib/boinc -type d -exec chown boinc:boinc {} \; 2>/dev/null || true
    find /var/lib/boinc -type f -exec chown boinc:boinc {} \; 2>/dev/null || true
}

# Запускаем оригинальную команду
exec "$@"

