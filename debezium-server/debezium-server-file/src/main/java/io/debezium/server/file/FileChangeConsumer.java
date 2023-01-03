/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.file;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.enterprise.context.Dependent;
import javax.inject.Named;

import org.apache.kafka.connect.data.Field;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.server.BaseChangeConsumer;

/**
 * Implementation of the consumer that delivers the messages to an HTTP Webhook destination.
 *
 * @author Chris Baumbauer
 */
@Named("file")
@Dependent
public class FileChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileChangeConsumer.class);

    private static final String PROP_PREFIX = "debezium.sink.filesink.";

    private boolean snapshotComplete = false;

    @ConfigProperty(name = PROP_PREFIX + "dataDir")
    String dataDir;

    Map<String, Table> tableMap = new HashMap<>();

    RecordParser parser;
    Map<Table, RecordWriter> snapshotWriters = new HashMap<>();
    RecordWriter cdcWriter;
    JsonFactory factory = new JsonFactory();
    ObjectMapper mapper = new ObjectMapper(factory);

    ExportStatus exportStatus;

    @PostConstruct
    void connect() throws URISyntaxException {
        LOGGER.info("connect() called: dataDir = {}", dataDir);

        parser = new JsonRecordParser(tableMap);
        exportStatus = ExportStatus.getInstance(dataDir);
        if (exportStatus.mode != null && exportStatus.mode.equals("streaming")) {
            handleSnapshotComplete();
        }
        else {
            exportStatus.mode = "snapshot";
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
        LOGGER.info("RECEIVED BATCH IN FILE SINK" + "Size of records-" + records.size());
        for (ChangeEvent<Object, Object> record : records) {
            Object objKey = record.key();
            Object objVal = record.value();
            // LOGGER.info("key type = {}, value type = {}", objKey.getClass().getName(), objVal.getClass().getName());
            if (objVal == null) {
                // tombstone event
                // TODO: handle this better. try using the config to avoid altogether
                continue;
            }

            // PARSE
            var r = parser.parseRecord(objKey, objVal);
            LOGGER.info("{} => {}", r.getTableIdentifier(), r.getValues());

            // WRITE
            RecordWriter writer = getWriterForRecord(r);
            writer.writeRecord(r);
            // Handle snapshot->cdc transition
            if (r.snapshot.equals("last")) {
                handleSnapshotComplete();
            }

            committer.markProcessed(record);
        }
        committer.markBatchFinished();

    }

    private RecordWriter getWriterForRecord(Record r) {
        if (!snapshotComplete) {
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
        snapshotComplete = true;
        exportStatus.mode = "streaming";
        closeSnapshotWriters();
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

class Table {
    String dbName, schemaName, tableName;
    LinkedHashMap<String, Field> fieldSchemas = new LinkedHashMap<>();

    @Override
    public String toString() {
        return dbName + "-" + schemaName + "-" + tableName;
    }

    public ArrayList<String> getColumns() {
        return new ArrayList<>(fieldSchemas.keySet());
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Table)) {
            return false;
        }
        Table t = (Table) o;
        return dbName.equals(t.dbName)
                && schemaName.equals(t.schemaName)
                && tableName.equals(t.tableName);
    }
}

class Record {
    Table t;
    String snapshot;
    String op;
    HashMap<String, Object> key = new HashMap<>();
    LinkedHashMap<String, Object> fields = new LinkedHashMap<>();

    public String getTableIdentifier() {
        return t.toString();
    }

    public ArrayList<Object> getValues() {
        return new ArrayList<>(fields.values());
    }
}
