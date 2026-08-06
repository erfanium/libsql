#!/bin/bash

set -Eeuo pipefail

SQLD_DB_PATH="${SQLD_DB_PATH:-iku.db}"
MINITURSO_DATA_DIR="${MINITURSO_DATA_DIR:-miniturso-data/platform}"
mkdir -p $SQLD_DB_PATH $MINITURSO_DATA_DIR
chown -R sqld:sqld $SQLD_DB_PATH $MINITURSO_DATA_DIR
exec gosu sqld docker-entrypoint.sh "$@"
