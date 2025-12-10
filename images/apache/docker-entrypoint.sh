#!/bin/bash
# Fix makeproject-step3.sh before starting (in case ONBUILD didn't run)
if [ -f /usr/bin/makeproject-step3.sh ]; then
    /usr/local/bin/fix-docker-group.sh || true
fi