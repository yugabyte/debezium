/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.ybexporter;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.enterprise.context.Dependent;
import javax.inject.Named;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.server.BaseChangeConsumer;

/**
 * Implementation of the consumer that exports the messages to file in a Yugabyte-compatible form.
 */
@Named("ybexporter")
@Dependent
public class YbExporterConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(YbExporterConsumer.class);
    private static final String PROP_PREFIX = "debezium.sink.ybexporter.";
    @ConfigProperty(name = PROP_PREFIX + "dataDir")
    String dataDir;

    private Map<String, Table> tableMap = new HashMap<>();
    private RecordParser parser;
    private Map<Table, RecordWriter> snapshotWriters = new HashMap<>();
    private RecordWriter cdcWriter;
    private ExportStatus exportStatus;

    @PostConstruct
    void connect() throws URISyntaxException {
        LOGGER.info("connect() called: dataDir = {}", dataDir);

        parser = new JsonRecordParser(tableMap);
        exportStatus = ExportStatus.getInstance(dataDir);
        if (exportStatus.getMode() != null && exportStatus.getMode().equals(ExportMode.STREAMING)) {
            handleSnapshotComplete();
        }
        else {
            exportStatus.updateMode(ExportMode.SNAPSHOT);
        }

        Thread t = new Thread(this::flush);
        t.setDaemon(true);
        t.start();
    }

    void flush() {
        LOGGER.info("XXX Started flush thread.");
        while (true) {
            for (RecordWriter writer : snapshotWriters.values()) {
                writer.flush();
            }
            if (cdcWriter != null) {
                cdcWriter.flush();
            }
            // TODO: doing more than flushing files to disk. maybe move this call to another thread?
            if (exportStatus != null) {
                exportStatus.flushToDisk();
            }
            try {
                Thread.sleep(2000);
            }
            catch (InterruptedException e) {
                // Noop.
            }
        }
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        LOGGER.info("Processing batch with {} records", records.size());
        for (ChangeEvent<Object, Object> record : records) {
            Object objKey = record.key();
            Object objVal = record.value();
            if (objVal == null) {
                // tombstone event
                // TODO: handle this better. try using the config to avoid altogether
                continue;
            }

            // PARSE
            var r = parser.parseRecord(objKey, objVal);
            LOGGER.info("Processing record {} => {}", r.getTableIdentifier(), r.getValueFieldValues());

            // WRITE
            RecordWriter writer = getWriterForRecord(r);
            writer.writeRecord(r);
            // Handle snapshot->cdc transition
            // String snapshot = ((Struct) ((SourceRecord) objVal).value()).getStruct("source").getString("snapshot");
            if (r.snapshot.equals("last")) {
                handleSnapshotComplete();
            }

            committer.markProcessed(record);
        }
        committer.markBatchFinished();

    }

    private RecordWriter getWriterForRecord(Record r) {
        if (exportStatus.getMode() == ExportMode.SNAPSHOT) {
            RecordWriter writer = snapshotWriters.get(r.t);
            if (writer == null) {
                writer = new TableSnapshotWriterCSV(dataDir, r.t);
                snapshotWriters.put(r.t, writer);
            }
            return writer;
        }
        else {
            return cdcWriter;
        }
    }

    private void handleSnapshotComplete() {
        exportStatus.updateMode(ExportMode.STREAMING);
        closeSnapshotWriters();
        Thread.currentThread().interrupt();
        openCDCWriter();
    }

    private void closeSnapshotWriters() {
        for (RecordWriter writer : snapshotWriters.values()) {
            writer.close();
        }
        snapshotWriters.clear();
    }

    private void openCDCWriter() {
        cdcWriter = new CDCWriterJson(dataDir);
    }
}
