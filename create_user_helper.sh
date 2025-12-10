#!/bin/bash
# Вспомогательный скрипт для создания пользователя через docker exec

EMAIL="$1"
PASSWD_HASH="$2"
USER_NAME="$3"

# Используем POST с правильным форматом данных
docker exec server-apache-1 curl -s -X POST \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "email_addr=${EMAIL}" \
  -d "passwd_hash=${PASSWD_HASH}" \
  -d "user_name=${USER_NAME}" \
  'http://localhost/boincserver/create_account.php'

