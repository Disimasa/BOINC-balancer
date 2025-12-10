#!/bin/bash
# Disable exit on error for docker group creation
sed -i 's/^set -e$/set +e/' /usr/bin/makeproject-step3.sh

# Fix docker group creation commands
sed -i 's/^addgroup -gid ${DOCKER_GID} docker$/getent group docker >\/dev\/null 2>\&1 || addgroup -gid ${DOCKER_GID} docker 2>\/dev\/null || true/' /usr/bin/makeproject-step3.sh
sed -i 's/^addgroup ${BOINC_USER} docker$/getent group docker >\/dev\/null 2>\&1 \&\& addgroup ${BOINC_USER} docker 2>\/dev\/null || true/' /usr/bin/makeproject-step3.sh

# Re-enable exit on error after docker group setup
sed -i '/^getent group docker >\/dev\/null 2>\&1 \&\& addgroup ${BOINC_USER} docker 2>\/dev\/null || true$/a set -e' /usr/bin/makeproject-step3.sh

