# Инструкция по созданию Apps и Tasks

## Шаг 1: Создание Apps

Сначала нужно создать 4 Apps в BOINC:

```bash
# Войти в контейнер apache
docker-compose exec apache bash

# Перейти в директорию проекта
cd /home/boincadm/project

# Запустить скрипт создания Apps
python3 /path/to/create_apps.py
```

Или вручную:

```bash
# Создать каждый App отдельно
boinc2docker_create_app --projhome /home/boincadm/project --appname fast_task --appver 1.0 --resultsdir /results/fast_task
boinc2docker_create_app --projhome /home/boincadm/project --appname medium_task --appver 1.0 --resultsdir /results/medium_task
boinc2docker_create_app --projhome /home/boincadm/project --appname long_task --appver 1.0 --resultsdir /results/long_task
boinc2docker_create_app --projhome /home/boincadm/project --appname random_task --appver 1.0 --resultsdir /results/random_task

# Обновить версии
bin/update_versions
```

## Шаг 2: Подготовка скриптов задач

Скрипты задач находятся в папке `tasks/`:
- `fast_task.py` - быстрая задача (~0.1-0.5 сек)
- `medium_task.py` - средняя задача (~1-2 сек)
- `long_task.py` - долгая задача (до 5 сек)
- `random_task.py` - задача со случайной сложностью

Эти скрипты нужно сделать доступными в Docker контейнере. Есть несколько вариантов:

### Вариант 1: Использовать готовый Docker образ с Python скриптами

Создайте Dockerfile для каждого типа задач или один общий:

```dockerfile
FROM python:alpine
COPY tasks/ /app/tasks/
WORKDIR /app
```

Затем соберите образ:
```bash
docker build -t my-tasks:latest .
```

### Вариант 2: Использовать встроенный код в команде

Измените `create_tasks.py` чтобы встраивать код скриптов прямо в команду.

## Шаг 3: Создание Tasks

После создания Apps, создайте задачи:

```bash
# Войти в контейнер
docker-compose exec apache bash
cd /home/boincadm/project

# Создать задачи для всех Apps (по умолчанию)
python3 /path/to/create_tasks.py

# Или создать задачи для конкретного App
python3 /path/to/create_tasks.py --app fast_task --count 30

# Или создать разное количество для каждого App
python3 /path/to/create_tasks.py --fast 20 --medium 15 --long 10 --random 15
```

## Проверка

Проверить созданные Apps и Tasks:

```bash
# Посмотреть все Apps
mysql -u boincadm -p$(cat /run/secrets/secrets.env | grep DB_PASSWD | cut -d= -f2) boincserver \
    -e "SELECT id, name, user_friendly_name FROM app;"

# Посмотреть количество задач для каждого App
mysql -u boincadm -p$(cat /run/secrets/secrets.env | grep DB_PASSWD | cut -d= -f2) boincserver \
    -e "SELECT app.name, COUNT(workunit.id) as task_count FROM app LEFT JOIN workunit ON app.id = workunit.appid GROUP BY app.id;"
```

## Примечания

- Скрипты задач должны быть доступны в Docker контейнере
- Результаты сохраняются в `/root/shared/results/` и автоматически возвращаются на сервер
- Каждая задача может быть выполнена несколько раз (параметр `target_nresults`)


