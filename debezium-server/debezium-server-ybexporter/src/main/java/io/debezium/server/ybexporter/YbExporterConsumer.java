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

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
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
    String snapshotMode;
    @ConfigProperty(name = PROP_PREFIX + "dataDir")
    String dataDir;

    String sourceType;
    private Map<String, Table> tableMap = new HashMap<>();
    private RecordParser parser;
    private Map<Table, RecordWriter> snapshotWriters = new HashMap<>();
    private RecordWriter streamingWriter;
    private ExportStatus exportStatus;
    Thread helperThread;

    @PostConstruct
    void connect() throws URISyntaxException {
        LOGGER.info("connect() called: dataDir = {}", dataDir);

        final Config config = ConfigProvider.getConfig();

        snapshotMode = config.getOptionalValue("debezium.source.snapshot.mode", String.class).orElse("");
        retrieveSourceType(config);

        parser = new KafkaConnectRecordParser(tableMap);
        exportStatus = ExportStatus.getInstance(dataDir);
        if (exportStatus.getMode() != null && exportStatus.getMode().equals(ExportMode.STREAMING)) {
            handleSnapshotComplete();
        }
        else {
            exportStatus.updateMode(ExportMode.SNAPSHOT);
        }

        helperThread = new Thread(this::flush);
        helperThread.setDaemon(true);
        helperThread.start();
    }

    void retrieveSourceType(Config config){
        String sourceConnector = config.getValue("debezium.source.connector.class", String.class);
        switch (sourceConnector){
            case "io.debezium.connector.postgresql.PostgresConnector":
                sourceType = "postgresql"; break;
            case "io.debezium.connector.oracle.OracleConnector":
                sourceType = "oracle"; break;
            case "io.debezium.connector.mysql.MySqlConnector":
                sourceType = "mysql"; break;
        }
    }

    void flush() {
        LOGGER.info("XXX Started flush thread.");
        while (true) {
            for (RecordWriter writer : snapshotWriters.values()) {
                writer.flush();
                writer.sync();
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
    public void handleBatch(List<ChangeEvent<Object, Object>> changeEvents, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        LOGGER.info("Processing batch with {} records", changeEvents.size());
        checkIfHelperThreadAlive();
        for (ChangeEvent<Object, Object> event : changeEvents) {
            Object objKey = event.key();
            Object objVal = event.value();

            // PARSE
            var r = parser.parseRecord(objKey, objVal);
            if (r.isUnsupported()) {
                committer.markProcessed(event);
                continue;
            }
            // LOGGER.info("Processing record {} => {}", r.getTableIdentifier(), r.getValueFieldValues());
            checkIfSnapshotAlreadyComplete(r);

            // WRITE
            RecordWriter writer = getWriterForRecord(r);
            writer.writeRecord(r);
            // Handle snapshot->cdc transition
            checkIfSnapshotComplete(r);

            committer.markProcessed(event);
        }
        handleBatchComplete();
        committer.markBatchFinished();
        handleSnapshotOnlyComplete();
    }

    private RecordWriter getWriterForRecord(Record r) {
        if (exportStatus.getMode() == ExportMode.SNAPSHOT) {
            RecordWriter writer = snapshotWriters.get(r.t);
            if (writer == null) {
                writer = new TableSnapshotWriterCSV(dataDir, r.t, sourceType);
                snapshotWriters.put(r.t, writer);
            }
            return writer;
        }
        else {
            return streamingWriter;
        }
    }

    /**
     * The last record we recieve will have the snapshot field='last'.
     * We interpret this to mean that snapshot phase is complete, and move on to streaming phase
     */
    private void checkIfSnapshotComplete(Record r) {
        if ((r.snapshot != null) && (r.snapshot.equals("last"))) {
            handleSnapshotComplete();
        }
    }

    /**
     * In an edge case where the last table scanned by debezium in the snapshot phase
     * has 0 rows, we do not get snapshot=last in the last record of the snapshot phase.
     * This is because debezium expected there to be more records in the subsequent table(s),
     * but the last table scanned ended up having 0 rows.
     *
     * To work around this, we check if we're still in snapshot phase, and if we get a record with snapshot=null
     * (which is indicative of streaming phase), we transition to streaming phase.
     * Note that this method would have to be called before the record is written.
     * @param r
     */
    private void checkIfSnapshotAlreadyComplete(Record r) {
        if ((exportStatus.getMode() == ExportMode.SNAPSHOT) && (r.snapshot == null)) {
            LOGGER.debug("Interpreting snapshot as complete since snapshot field of record is null");
            handleSnapshotComplete();
        }
    }

    private void handleSnapshotComplete() {
        exportStatus.updateMode(ExportMode.STREAMING);
        closeSnapshotWriters();
        // Thread.currentThread().interrupt(); // For testing
        openCDCWriter();
    }

    private void handleSnapshotOnlyComplete() {
        if ((exportStatus.getMode() == ExportMode.STREAMING) && (snapshotMode.equals("initial_only"))) {
            LOGGER.info("Snapshot complete. Interrupting thread as snapshot mode = initial_only");
            exportStatus.flushToDisk();
            Thread.currentThread().interrupt();
        }
    }

    private void closeSnapshotWriters() {
        for (RecordWriter writer : snapshotWriters.values()) {
            writer.close();
        }
        snapshotWriters.clear();
    }

    private void handleBatchComplete() {
        flushSyncStreamingData();
    }

    /**
     * At the end of batch, we sync streaming data to storage.
     * This is inline with debezium behavior - https://debezium.io/documentation/reference/stable/development/engine.html#_handling_failures
     * In case machine powers off before data is synced to storage, those events will be received again upon restart
     * because debezium flushes its offsets information at the end of every batch.
     */
    private void flushSyncStreamingData() {
        if (exportStatus.getMode().equals(ExportMode.STREAMING)) {
            if (streamingWriter != null) {
                streamingWriter.flush();
                streamingWriter.sync();
            }
        }
    }

    private void openCDCWriter() {
        streamingWriter = new StreamingWriterJson(dataDir);
    }

    private void checkIfHelperThreadAlive(){
        if (!helperThread.isAlive()){
            // if helper thread dies, export status will stop being updated,
            // so interrupting main thread as well.
            throw new RuntimeException("Helper Thread exited.");
        }
    }
}
