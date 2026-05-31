FROM eclipse-temurin:21-jre

ARG NB5_URL=https://github.com/nosqlbench/nosqlbench/releases/download/5.25.10-release/nb5

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl procps sysstat time jq \
    && rm -rf /var/lib/apt/lists/* \
    && curl -LfsS "$NB5_URL" -o /usr/local/bin/nb5 \
    && chmod +x /usr/local/bin/nb5 \
    && mkdir -p /usr/local/share/nosqlbench

COPY run-nb.sh /usr/local/bin/run-nb
COPY cql_iot_append.yaml /usr/local/share/nosqlbench/cql_iot_append.yaml

RUN chmod +x /usr/local/bin/run-nb \
    && mkdir -p /results

WORKDIR /results
CMD ["sleep", "infinity"]
