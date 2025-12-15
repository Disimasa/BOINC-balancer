#!/bin/bash
set -e

# Создаем необходимые директории и устанавливаем права
mkdir -p /var/lib/boinc/slots /var/lib/boinc/projects
mkdir -p /var/lib/boinc/projects/172.26.176.1_boincserver
mkdir -p /var/lib/boinc/projects/boincserver_boincserver
mkdir -p /var/lib/boinc/projects/localhost_boincserver

# Принудительно устанавливаем права на все директории (включая существующие в volume)
chmod -R 755 /var/lib/boinc 2>/dev/null || true
chown -R boinc:boinc /var/lib/boinc 2>/dev/null || true

# Запускаем оригинальную команду
exec "$@"

