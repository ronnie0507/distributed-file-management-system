FROM quay.io/coreos/etcd:v3.5.21 AS etcd

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=etcd /usr/local/bin/etcdctl /usr/local/bin/etcdctl
COPY bootstrap.sh /usr/local/bin/bootstrap.sh
RUN chmod +x /usr/local/bin/bootstrap.sh
ENTRYPOINT ["/usr/local/bin/bootstrap.sh"]
