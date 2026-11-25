# On your terminal, run the following to build the image:
# mvn clean package -Dquick

FROM debezium/connect:2.5.2.Final

# Install Java 17 (required because connector JAR is compiled with Java 17)
USER root
RUN microdnf install -y java-17-openjdk-headless && microdnf clean all \
    && ln -sfn /usr/lib/jvm/java-17-openjdk-* /usr/lib/jvm/java-17
ENV JAVA_HOME=/usr/lib/jvm/java-17
ENV PATH=$JAVA_HOME/bin:$PATH

# Apache Kafka 4.3.2 replaces the base image's Kafka 3.6.1. No Debezium image ships Kafka 4.3.2, so the release is
# installed here, as in the gRPC connector's base image, and checked against its published SHA-512. Of Kafka's
# files, the base image adds only the Jolokia agent jar, which is kept. The base image's entrypoint is unchanged, and
# Kafka 4.x still reads log4j.properties, through its log4j 1.x bridge. As in Debezium's Kafka 4.x images,
# config.orig holds the Kafka 4.3.2 config files plus log4j.properties.
ENV KAFKA_VERSION=4.3.2
ARG KAFKA_SHA512=c2063485f90312bba795052df3e49b8eea337adf97f1111b946c60771c7a2f088055d2ccc7221d72fe30bd6a3f7a58348f14e14240eaddfe60446ea1c033618b
RUN curl -fsSLo /tmp/kafka.tgz https://dlcdn.apache.org/kafka/$KAFKA_VERSION/kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz \
      || curl -fsSLo /tmp/kafka.tgz https://archive.apache.org/dist/kafka/$KAFKA_VERSION/kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz \
    && echo "$KAFKA_SHA512  /tmp/kafka.tgz" | sha512sum -c - \
    && mkdir /tmp/kafka && tar -xzf /tmp/kafka.tgz -C /tmp/kafka --strip-components=1 \
    && mv $KAFKA_HOME/libs/jolokia-jvm-$JOLOKIA_VERSION.jar /tmp/kafka/libs/ \
    && rm -rf $KAFKA_HOME/libs $KAFKA_HOME/bin $KAFKA_HOME/licenses \
    && mv /tmp/kafka/libs /tmp/kafka/bin /tmp/kafka/licenses /tmp/kafka/LICENSE /tmp/kafka/NOTICE $KAFKA_HOME/ \
    && find $KAFKA_HOME/config.orig -mindepth 1 -maxdepth 1 ! -name log4j.properties -exec rm -rf {} + \
    && cp /tmp/kafka/config/* $KAFKA_HOME/config.orig/ \
    && chown -R root:root $KAFKA_HOME/libs $KAFKA_HOME/bin $KAFKA_HOME/licenses $KAFKA_HOME/LICENSE $KAFKA_HOME/NOTICE \
    && chown -R kafka:kafka $KAFKA_HOME/config.orig \
    && chmod -R a+rwX $KAFKA_HOME/libs $KAFKA_HOME/bin $KAFKA_HOME/licenses $KAFKA_HOME/config.orig \
    && chmod a+rw $KAFKA_HOME/LICENSE $KAFKA_HOME/NOTICE \
    && rm -rf /tmp/kafka /tmp/kafka.tgz
USER kafka

# Debezium's Kafka Connect REST extension (ENABLE_DEBEZIUM_KC_REST_EXTENSION=true) is built on javax.ws.rs, which
# Kafka 4.x no longer has, and the worker does not start with it. Debezium's own Kafka 4.x images no longer ship it,
# and without the directory the switch is ignored.
RUN rm -rf $EXTERNAL_LIBS_DIR/debezium-connect-rest-extension

WORKDIR $KAFKA_CONNECT_PLUGINS_DIR
RUN rm -f debezium-connector-postgres/debezium-connector-postgres-*.jar
# The connector jar bundles pgjdbc; the base image's older driver would load first and shadow it
RUN rm -f debezium-connector-postgres/postgresql-*.jar
# The connector jar bundles its own debezium-api, debezium-core and protobuf-java, with the changes for Kafka 4.x and
# protobuf 3.25.5. Connect loads a plugin's jars in file name order, so the base image's copies (Debezium 2.5.2 as
# released, protobuf 3.25.2) would load first.
RUN rm -f debezium-connector-postgres/debezium-api-*.jar debezium-connector-postgres/debezium-core-*.jar debezium-connector-postgres/protobuf-java-*.jar
RUN rm -rf debezium-connector-db2
RUN rm -rf debezium-connector-informix
RUN rm -rf debezium-connector-mongodb
RUN rm -rf debezium-connector-jdbc
RUN rm -rf debezium-connector-mysql
RUN rm -rf debezium-connector-oracle
RUN rm -rf debezium-connector-spanner
RUN rm -rf debezium-connector-sqlserver
RUN rm -rf debezium-connector-vitess
WORKDIR /

# Copy the Debezium Connector for Postgres adapted for YugabyteDB
COPY debezium-connector-postgres/target/yugabytedb-source-connector-*.jar $KAFKA_CONNECT_PLUGINS_DIR/debezium-connector-postgres/

# Applies to every TLS client in the JVM, including the connection to YugabyteDB
ENV KAFKA_OPTS="-Djdk.tls.client.protocols=TLSv1.3,TLSv1.2 -javaagent:/kafka/etc/jmx_prometheus_javaagent-0.17.2.jar=8080:/kafka/etc/jmx-exporter/metrics.yml"

# Package all the Avro related jar files
RUN mkdir -p $KAFKA_CONNECT_PLUGINS_DIR/debezium-connector-postgres/avro-supporting-jars/
WORKDIR $KAFKA_CONNECT_PLUGINS_DIR/debezium-connector-postgres/avro-supporting-jars
RUN curl -sLo kafka-connect-avro-converter-7.5.3.jar https://packages.confluent.io/maven/io/confluent/kafka-connect-avro-converter/7.5.3/kafka-connect-avro-converter-7.5.3.jar 
RUN curl -sLo kafka-connect-avro-data-7.5.3.jar https://packages.confluent.io/maven/io/confluent/kafka-connect-avro-data/7.5.3/kafka-connect-avro-data-7.5.3.jar
RUN curl -sLo kafka-avro-serializer-7.5.3.jar https://packages.confluent.io/maven/io/confluent/kafka-avro-serializer/7.5.3/kafka-avro-serializer-7.5.3.jar
RUN curl -sLo kafka-schema-serializer-7.5.3.jar https://packages.confluent.io/maven/io/confluent/kafka-schema-serializer/7.5.3/kafka-schema-serializer-7.5.3.jar 
RUN curl -sLo kafka-schema-converter-7.5.3.jar https://packages.confluent.io/maven/io/confluent/kafka-schema-converter/7.5.3/kafka-schema-converter-7.5.3.jar
RUN curl -sLo kafka-schema-registry-client-7.5.3.jar https://packages.confluent.io/maven/io/confluent/kafka-schema-registry-client/7.5.3/kafka-schema-registry-client-7.5.3.jar 
RUN curl -sLo common-config-7.5.3.jar https://packages.confluent.io/maven/io/confluent/common-config/7.5.3/common-config-7.5.3.jar
RUN curl -sLo common-utils-7.5.3.jar https://packages.confluent.io/maven/io/confluent/common-utils/7.5.3/common-utils-7.5.3.jar 

RUN curl -sLo avro-1.11.3.jar https://repo1.maven.org/maven2/org/apache/avro/avro/1.11.3/avro-1.11.3.jar
RUN curl -sLo commons-compress-1.21.jar https://repo1.maven.org/maven2/org/apache/commons/commons-compress/1.21/commons-compress-1.21.jar
RUN curl -sLo failureaccess-1.0.jar https://repo1.maven.org/maven2/com/google/guava/failureaccess/1.0/failureaccess-1.0.jar
RUN curl -sLo guava-33.0.0-jre.jar https://repo1.maven.org/maven2/com/google/guava/guava/33.0.0-jre/guava-33.0.0-jre.jar 
RUN curl -sLo minimal-json-0.9.5.jar https://repo1.maven.org/maven2/com/eclipsesource/minimal-json/minimal-json/0.9.5/minimal-json-0.9.5.jar
RUN curl -sLo re2j-1.6.jar https://repo1.maven.org/maven2/com/google/re2j/re2j/1.6/re2j-1.6.jar 
RUN curl -sLo slf4j-api-1.7.36.jar https://repo1.maven.org/maven2/org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar
RUN curl -sLo snakeyaml-2.0.jar https://repo1.maven.org/maven2/org/yaml/snakeyaml/2.0/snakeyaml-2.0.jar 
RUN curl -sLo swagger-annotations-2.1.10.jar https://repo1.maven.org/maven2/io/swagger/core/v3/swagger-annotations/2.1.10/swagger-annotations-2.1.10.jar 
RUN curl -sLo jackson-databind-2.21.8.jar https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.21.8/jackson-databind-2.21.8.jar 
RUN curl -sLo jackson-core-2.21.8.jar https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.21.8/jackson-core-2.21.8.jar 
RUN curl -sLo jackson-annotations-2.21.jar https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/2.21/jackson-annotations-2.21.jar 
RUN curl -sLo jackson-dataformat-csv-2.21.8.jar https://repo1.maven.org/maven2/com/fasterxml/jackson/dataformat/jackson-dataformat-csv/2.21.8/jackson-dataformat-csv-2.21.8.jar 
RUN curl -sLo logredactor-1.0.12.jar https://repo1.maven.org/maven2/io/confluent/logredactor/1.0.12/logredactor-1.0.12.jar
RUN curl -sLo logredactor-metrics-1.0.12.jar https://repo1.maven.org/maven2/io/confluent/logredactor-metrics/1.0.12/logredactor-metrics-1.0.12.jar
WORKDIR /

# Add the required jar files to be packaged with the base connector
RUN cd $KAFKA_CONNECT_PLUGINS_DIR/debezium-connector-postgres && curl -sLo kafka-connect-jdbc-10.6.5-CUSTOM.8.jar https://github.com/yugabyte/kafka-connect-jdbc/releases/download/10.6.5-CUSTOM.8/kafka-connect-jdbc-10.6.5-CUSTOM.8.jar
RUN cd $KAFKA_CONNECT_PLUGINS_DIR/debezium-connector-postgres && curl -sLo jdbc-yugabytedb-42.7.3-yb-7.jar https://repo1.maven.org/maven2/com/yugabyte/jdbc-yugabytedb/42.7.3-yb-7/jdbc-yugabytedb-42.7.3-yb-7.jar
RUN cd $KAFKA_CONNECT_PLUGINS_DIR/debezium-connector-postgres && curl -sLo transforms-for-apache-kafka-connect-1.5.0.zip https://github.com/Aiven-Open/transforms-for-apache-kafka-connect/releases/download/v1.5.0/transforms-for-apache-kafka-connect-1.5.0.zip
RUN cd $KAFKA_CONNECT_PLUGINS_DIR/debezium-connector-postgres && unzip transforms-for-apache-kafka-connect-1.5.0.zip

# Add Jmx agent and metrics pattern file to expose the metrics info
RUN mkdir /kafka/etc && cd /kafka/etc && curl -so jmx_prometheus_javaagent-0.17.2.jar https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.17.2/jmx_prometheus_javaagent-0.17.2.jar

COPY metrics.yml /kafka/etc/jmx-exporter/

ENV CLASSPATH=$KAFKA_HOME
ENV JMXHOST=localhost
ENV JMXPORT=1976

# properties file having instructions to roll over log files in case the size exceeds a given limit
COPY log4j.properties $KAFKA_HOME/config/log4j.properties

