FROM boinc/client:latest

# Копируем entrypoint скрипт, который создаст директории при запуске
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Копируем app_config.xml в нужные места
COPY app_config.xml /var/lib/boinc/app_config.xml
RUN mkdir -p /var/lib/boinc/projects/172.26.176.1_boincserver && \
    cp /var/lib/boinc/app_config.xml /var/lib/boinc/projects/172.26.176.1_boincserver/app_config.xml

ENTRYPOINT ["/entrypoint.sh"]
CMD ["start-boinc.sh"]

