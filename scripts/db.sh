#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

PG_BIN="${PG_BIN:-$(brew --prefix postgresql@18)/bin}"
DATA=.pgdata
PORT=5544

case "${1:-}" in
up)
  if [ ! -d "$DATA" ]; then
    "$PG_BIN/initdb" -D "$DATA" --auth=trust --username=postgres --encoding=UTF8 >/dev/null
  fi
  if ! "$PG_BIN/pg_ctl" -D "$DATA" status >/dev/null 2>&1; then
    "$PG_BIN/pg_ctl" -D "$DATA" -l "$DATA/server.log" -w start \
      -o "-p $PORT -c listen_addresses=localhost -c unix_socket_directories='' -c max_connections=300 \
          -c tcp_keepalives_idle=10 -c tcp_keepalives_interval=5 -c tcp_keepalives_count=3 \
          -c wal_sync_method=fsync_writethrough"
  fi
  "$PG_BIN/psql" -h localhost -p "$PORT" -U postgres -tAc \
    "SELECT 1 FROM pg_database WHERE datname = 'triggerinbox'" | grep -q 1 ||
    "$PG_BIN/createdb" -h localhost -p "$PORT" -U postgres triggerinbox
  echo "Postgres is running on localhost:$PORT"
  ;;
down)
  "$PG_BIN/pg_ctl" -D "$DATA" -w stop
  ;;
psql)
  exec "$PG_BIN/psql" -h localhost -p "$PORT" -U postgres triggerinbox
  ;;
*)
  echo "usage: $0 up|down|psql" >&2
  exit 1
  ;;
esac
