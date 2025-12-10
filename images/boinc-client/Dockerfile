FROM boinc/client:latest

# Создаем необходимые директории и устанавливаем права при билде
RUN mkdir -p /var/lib/boinc/slots /var/lib/boinc/projects && \
    chmod -R 755 /var/lib/boinc && \
    chown -R boinc:boinc /var/lib/boinc 2>/dev/null || true

# Копируем entrypoint скрипт для исправления прав доступа
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Используем наш entrypoint, который исправляет права и затем запускает стандартную команду
ENTRYPOINT ["/entrypoint.sh"]
# Сохраняем стандартную команду образа
CMD ["start-boinc.sh"]

