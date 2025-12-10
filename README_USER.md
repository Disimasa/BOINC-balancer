# Создание пользователя и подключение клиентов

## Настройка

1. Создайте файл `.env` в директории `server/`:

```bash
# BOINC Project Configuration
PROJECT_URL=http://172.26.176.1/boincserver

# User credentials for BOINC account
BOINC_USER_EMAIL=test@test.com
BOINC_USER_PASSWORD=testpass
BOINC_USER_NAME=TestUser

# Account key (будет заполнен автоматически после создания пользователя)
BOINC_ACCOUNT_KEY=
```

2. Установите зависимости (если еще не установлены):

```bash
pip install python-dotenv requests
```

## Создание пользователя

Запустите скрипт для создания пользователя:

```bash
python create_user.py
```

Скрипт:
- Проверит, существует ли пользователь с указанным email
- Если пользователь существует, получит его account key
- Если пользователя нет, создаст нового и получит account key
- Выведет account key, который нужно добавить в `.env`

После получения account key, добавьте его в `.env`:

```
BOINC_ACCOUNT_KEY=ваш_account_key_здесь
```

## Подключение клиентов

После создания пользователя и добавления account key в `.env`, запустите:

```bash
python connect_clients.py
```

Скрипт подключит все клиенты к проекту, используя account key из `.env`.

## Альтернативный способ (через веб-интерфейс)

1. Откройте в браузере: `http://172.26.176.1/boincserver/user/signup.php`
2. Заполните форму регистрации
3. После регистрации откройте: `http://172.26.176.1/boincserver/user/weak_auth.php`
4. Скопируйте account key и добавьте в `.env`

