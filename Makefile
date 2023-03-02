.PHONY: debezium debezium-server yb-exporter debezium-server-core postgres-connector debezium-core

VERSION=2.2.0
DEBEZIUM_SERVER_LIB=debezium-server/debezium-server-dist/target/debezium-server/lib/

debezium:
	mvn clean install -Dquick

debezium-server:
	cd debezium-server/debezium-server-dist; \
	mvn clean install -Passembly; \
	cd target; \
	tar -xvzf debezium-server-dist-$(VERSION)-SNAPSHOT.tar.gz;

debezium-server-core:
	mvn clean install -Dquick -pl :debezium-server-core; \
	cp debezium-server/debezium-server-core/target/debezium-server-core-$(VERSION)-SNAPSHOT.jar $(DEBEZIUM_SERVER_LIB);

yb-exporter:
	mvn clean install -Dquick -pl :debezium-server-ybexporter; \
	cp debezium-server/debezium-server-ybexporter/target/debezium-server-ybexporter-$(VERSION)-SNAPSHOT.jar $(DEBEZIUM_SERVER_LIB);

postgres-connector:
	mvn clean install -Dquick -pl :debezium-connector-postgres; \
	cp debezium-connector-postgres/target/debezium-connector-postgres-$(VERSION)-SNAPSHOT.jar $(DEBEZIUM_SERVER_LIB);

debezium-core:
	mvn clean install -Dquick -pl :debezium-core; \
	cp debezium-core/target/debezium-core-$(VERSION)-SNAPSHOT.jar $(DEBEZIUM_SERVER_LIB);
