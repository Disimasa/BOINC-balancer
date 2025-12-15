FROM boinc/server_apache:4.0.1-b2d

# Обновляем Docker CLI до версии, совместимой с современным Docker daemon (API 1.44+)
# Используем Docker CLI 25.0.0 - поддерживает API 1.44+
RUN curl -fsSL https://download.docker.com/linux/static/stable/x86_64/docker-25.0.0.tgz | \
    tar -xz -C /tmp && \
    mv /tmp/docker/docker /usr/bin/docker && \
    chmod +x /usr/bin/docker && \
    rm -rf /tmp/docker && \
    docker --version

# Копируем ПАТЧ-скрипт как fix-docker-group.sh
COPY fix-docker-group.sh /usr/local/bin/fix-docker-group.sh
RUN chmod +x /usr/local/bin/fix-docker-group.sh

# Копируем ENTRYPOINT как docker-entrypoint.sh
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

ONBUILD RUN /usr/local/bin/fix-docker-group.sh || true
ONBUILD ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]