FROM boinc/client:latest

# Копируем entrypoint скрипт, который создаст директории при запуске
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["start-boinc.sh"]

