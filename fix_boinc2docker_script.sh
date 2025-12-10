#!/bin/bash
# Исправление окончаний строк для boinc2docker_create_app

docker-compose exec -T apache bash -c '
cd /home/boincadm/project/bin
if [ -f boinc2docker_create_app ]; then
    tr -d "\r" < boinc2docker_create_app > boinc2docker_create_app.tmp
    mv boinc2docker_create_app.tmp boinc2docker_create_app
    chmod +x boinc2docker_create_app
    echo "Fixed line endings"
fi
'

