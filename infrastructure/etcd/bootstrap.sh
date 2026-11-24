#!/bin/sh
set -eu

: "${ETCD_ROOT_PASSWORD:?ETCD_ROOT_PASSWORD is required}"
: "${ETCD_USERNAME:?ETCD_USERNAME is required}"
: "${ETCD_PASSWORD:?ETCD_PASSWORD is required}"
if [ "$ETCD_ROOT_PASSWORD" = "$ETCD_PASSWORD" ]; then
    echo "ETCD_ROOT_PASSWORD and ETCD_PASSWORD must be different" >&2
    exit 1
fi

export ETCDCTL_API=3
endpoint="http://etcd-1:2379"

ready=0
attempt=0
while [ "$attempt" -lt 60 ]; do
    if etcdctl --endpoints="$endpoint" endpoint health >/dev/null 2>&1; then
        ready=1
        break
    fi
    attempt=$((attempt + 1))
    sleep 2
done
if [ "$ready" -ne 1 ]; then
    echo "etcd did not become healthy before auth bootstrap" >&2
    exit 1
fi

auth_status="$(
    etcdctl --endpoints="$endpoint" \
        --user="root:${ETCD_ROOT_PASSWORD}" \
        auth status --write-out=json
)"
if printf '%s' "$auth_status" | grep -Eq '"enabled"[[:space:]]*:[[:space:]]*true'; then
    exit 0
fi

if ! etcdctl --endpoints="$endpoint" user get root >/dev/null 2>&1; then
    etcdctl --endpoints="$endpoint" user add "root:${ETCD_ROOT_PASSWORD}"
fi
if ! etcdctl --endpoints="$endpoint" role get root >/dev/null 2>&1; then
    etcdctl --endpoints="$endpoint" role add root
fi
etcdctl --endpoints="$endpoint" \
    --user="root:${ETCD_ROOT_PASSWORD}" \
    user grant-role root root

if ! etcdctl --endpoints="$endpoint" role get dfms-worker >/dev/null 2>&1; then
    etcdctl --endpoints="$endpoint" \
        --user="root:${ETCD_ROOT_PASSWORD}" \
        role add dfms-worker
fi
etcdctl --endpoints="$endpoint" \
    --user="root:${ETCD_ROOT_PASSWORD}" \
    role grant-permission dfms-worker readwrite /dfms/ --prefix

if ! etcdctl --endpoints="$endpoint" \
    --user="root:${ETCD_ROOT_PASSWORD}" \
    user get "$ETCD_USERNAME" >/dev/null 2>&1; then
    etcdctl --endpoints="$endpoint" \
        --user="root:${ETCD_ROOT_PASSWORD}" \
        user add "${ETCD_USERNAME}:${ETCD_PASSWORD}"
fi
etcdctl --endpoints="$endpoint" \
    --user="root:${ETCD_ROOT_PASSWORD}" \
    user grant-role "$ETCD_USERNAME" dfms-worker
etcdctl --endpoints="$endpoint" \
    --user="root:${ETCD_ROOT_PASSWORD}" \
    auth enable
