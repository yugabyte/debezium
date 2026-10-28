# On your terminal, run the following to build the image:
# mvn clean package -Dquick

# Kafka Connect 4.3.1 on Java 25, with the same /kafka layout, entrypoint and environment
# variables as the debezium/connect:2.5.2.Final image used before
FROM quay.io/debezium/connect:3.7.0.Final

# Remove every upstream connector. Upstream's debezium-connector-postgres directory holds the
# Debezium 3.x core jars, which must not share a class loader with the YugabyteDB connector.
RUN rm -rf $KAFKA_CONNECT_PLUGINS_DIR/* && mkdir $KAFKA_CONNECT_PLUGINS_DIR/debezium-connector-postgres

# Copy the Debezium Connector for Postgres adapted for YugabyteDB
COPY debezium-connector-postgres/target/yugabytedb-source-connector-*-jar-with-dependencies.jar $KAFKA_CONNECT_PLUGINS_DIR/debezium-connector-postgres/

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
RUN curl -sLo jackson-databind-2.14.2.jar https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.14.2/jackson-databind-2.14.2.jar 
RUN curl -sLo jackson-core-2.14.2.jar https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.14.2/jackson-core-2.14.2.jar 
RUN curl -sLo jackson-annotations-2.14.2.jar https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/2.14.2/jackson-annotations-2.14.2.jar 
RUN curl -sLo jackson-dataformat-csv-2.14.2.jar https://repo1.maven.org/maven2/com/fasterxml/jackson/dataformat/jackson-dataformat-csv/2.14.2/jackson-dataformat-csv-2.14.2.jar 
RUN curl -sLo logredactor-1.0.12.jar https://repo1.maven.org/maven2/io/confluent/logredactor/1.0.12/logredactor-1.0.12.jar
RUN curl -sLo logredactor-metrics-1.0.12.jar https://repo1.maven.org/maven2/io/confluent/logredactor-metrics/1.0.12/logredactor-metrics-1.0.12.jar
WORKDIR /

# Add the required jar files to be packaged with the base connector
RUN cd $KAFKA_CONNECT_PLUGINS_DIR/debezium-connector-postgres && curl -sLo kafka-connect-jdbc-10.6.5-CUSTOM.8.jar https://github.com/yugabyte/kafka-connect-jdbc/releases/download/10.6.5-CUSTOM.8/kafka-connect-jdbc-10.6.5-CUSTOM.8.jar
RUN cd $KAFKA_CONNECT_PLUGINS_DIR/debezium-connector-postgres && curl -sLo jdbc-yugabytedb-42.7.3-yb-5.jar https://repo1.maven.org/maven2/com/yugabyte/jdbc-yugabytedb/42.7.3-yb-5/jdbc-yugabytedb-42.7.3-yb-5.jar
RUN cd $KAFKA_CONNECT_PLUGINS_DIR/debezium-connector-postgres && curl -sLo transforms-for-apache-kafka-connect-1.5.0.zip https://github.com/Aiven-Open/transforms-for-apache-kafka-connect/releases/download/v1.5.0/transforms-for-apache-kafka-connect-1.5.0.zip
RUN cd $KAFKA_CONNECT_PLUGINS_DIR/debezium-connector-postgres && unzip transforms-for-apache-kafka-connect-1.5.0.zip

# The Aiven transforms and the Confluent Avro converter ship without ServiceLoader manifests, so Kafka Connect
# 4.x warns about them at startup and cannot load them with plugin.discovery=service_load. Generate the manifests.
# KAFKA_OPTS is cleared because the JMX agent it loads is only downloaded further down.
RUN KAFKA_OPTS= $KAFKA_HOME/bin/connect-plugin-path.sh sync-manifests --plugin-path $KAFKA_CONNECT_PLUGINS_DIR

# Add Jmx agent and metrics pattern file to expose the metrics info
RUN mkdir /kafka/etc && cd /kafka/etc && curl -so jmx_prometheus_javaagent-0.17.2.jar https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.17.2/jmx_prometheus_javaagent-0.17.2.jar

COPY metrics.yml /kafka/etc/jmx-exporter/

ENV CLASSPATH=$KAFKA_HOME
ENV JMXHOST=localhost
ENV JMXPORT=1976

# properties file having instructions to roll over log files in case the size exceeds a given limit
COPY log4j.properties $KAFKA_HOME/config/log4j.properties

