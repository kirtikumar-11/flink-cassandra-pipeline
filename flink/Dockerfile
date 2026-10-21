# Flink 1.20 with Python (PyFlink) and the Kafka + Cassandra connectors baked in.
# Same image for the JobManager and the TaskManagers, so Python code runs the same everywhere.
FROM flink:1.20.1-scala_2.12-java17

RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-pip python3-dev \
 && ln -sf /usr/bin/python3 /usr/bin/python \
 && rm -rf /var/lib/apt/lists/*

# PyFlink must match the Flink version exactly. The other two are for the producer and
# verify script, so they can run in a container too ("tools" service in docker-compose.yml).
RUN pip3 install --no-cache-dir apache-flink==1.20.1 confluent-kafka==2.6.1 cassandra-driver==3.29.2

# Connector jars go into Flink's lib/ so every job can use them.
ARG MAVEN=https://repo1.maven.org/maven2/org/apache/flink
RUN wget -q -P /opt/flink/lib \
      ${MAVEN}/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar \
      ${MAVEN}/flink-connector-cassandra_2.12/3.2.0-1.19/flink-connector-cassandra_2.12-3.2.0-1.19.jar

# Checkpoints and savepoints live here (a Docker volume). Flink runs as the "flink" user.
RUN mkdir -p /flink-data && chown flink:flink /flink-data

COPY jobs /opt/jobs
