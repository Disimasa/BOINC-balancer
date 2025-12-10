#!/bin/bash
# Принудительное обновление версий для всех приложений

cd /home/boincadm/project

echo "Принудительно обновляю версии для всех приложений..."

# Сначала убедимся, что все приложения зарегистрированы
bin/xadd 2>&1 | tail -10

# Теперь обновим версии, игнорируя ошибки подписи
echo ""
echo "Обновляю версии (может быть несколько ошибок подписи, это нормально)..."
yes | bin/update_versions 2>&1 | grep -v "Error: no .sig file" | head -100

echo ""
echo "Проверяю, какие версии были найдены..."
bin/update_versions 2>&1 | grep "Found app version directory" | head -20

echo ""
echo "Готово! Перезапустите демоны BOINC: bin/stop && bin/start"


