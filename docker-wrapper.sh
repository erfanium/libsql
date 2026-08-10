#!/bin/bash

set -Eeuo pipefail

SQLD_DB_PATH="${SQLD_DB_PATH:-iku.db}"
ADMIN_DATA_DIR="${ADMIN_DATA_DIR:-admin-data/platform}"
mkdir -p $SQLD_DB_PATH $ADMIN_DATA_DIR
chown -R sqld:sqld $SQLD_DB_PATH $ADMIN_DATA_DIR
exec gosu sqld docker-entrypoint.sh "$@"
