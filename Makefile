.PHONY: debezium debezium-server yb-exporter

debezium:
	mvn clean install -Pquick

debezium-server:
	cd debezium-server/debezium-server-dist; \
	mvn clean install -Passembly; \
	cd target; \
	tar -xvzf debezium-server-dist-2.1.0-SNAPSHOT.tar.gz;

debezium-server-core:
    mvn clean install -Pquick -pl :debezium-server-core; \
    cp debezium-server/debezium-server-core/target/debezium-server-core-2.1.0-SNAPSHOT.jar debezium-server/debezium-server-dist/target/debezium-server/lib/;

yb-exporter:
	mvn clean install -Pquick -pl :debezium-server-ybexporter; \
	cp debezium-server/debezium-server-ybexporter/target/debezium-server-ybexporter-2.1.0-SNAPSHOT.jar debezium-server/debezium-server-dist/target/debezium-server/lib/;