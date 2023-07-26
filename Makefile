.PHONY: debezium debezium-server yb-exporter debezium-server-core postgres-connector debezium-core

VERSION=1.9.5.Final
DEBEZIUM_SERVER_LIB=debezium-server/debezium-server-dist/target/debezium-server/lib/

debezium:
	mvn -ntp clean install -Dquick -Pquick

debezium-server:
	cd debezium-server/debezium-server-dist; \
	mvn -ntp clean install -Passembly; \
	cd target; \
	tar -xvzf debezium-server-dist-$(VERSION).tar.gz;
	cd debezium-server/lib
	wget -nv https://github.com/yugabyte/debezium-connector-yugabytedb/releases/download/v1.9.5.y.31/debezium-connector-yugabytedb-1.9.5.y.31.jar

debezium-server-core:
	mvn -ntp clean install -Dquick -Pquick -pl :debezium-server-core; \
	cp debezium-server/debezium-server-core/target/debezium-server-core-$(VERSION).jar $(DEBEZIUM_SERVER_LIB);

yb-exporter:
	mvn -ntp clean install -Dquick -Pquick -pl :debezium-server-ybexporter; \
	cp debezium-server/debezium-server-ybexporter/target/debezium-server-ybexporter-$(VERSION).jar $(DEBEZIUM_SERVER_LIB);

postgres-connector:
	mvn -ntp clean install -Dquick -Pquick -pl :debezium-connector-postgres; \
	cp debezium-connector-postgres/target/debezium-connector-postgres-$(VERSION).jar $(DEBEZIUM_SERVER_LIB);

debezium-core:
	mvn -ntp clean install -Dquick -Pquick -pl :debezium-core; \
	cp debezium-core/target/debezium-core-$(VERSION).jar $(DEBEZIUM_SERVER_LIB);
