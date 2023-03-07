/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.ybexporter;

import java.io.BufferedWriter;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.SyncFailedException;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

public class StreamingWriterJson implements RecordWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableSnapshotWriterCSV.class);
    private static final String QUEUE_FILE_NAME = "queue.json";
    private String dataDir;
    private BufferedWriter writer;
    private ObjectWriter ow;
    private FileOutputStream fos;
    private FileDescriptor fd;
    private SequenceNumberGenerator seqNumGen;

    public StreamingWriterJson(String datadirStr) {
        dataDir = datadirStr;

        var fileName = String.format("%s/%s", dataDir, QUEUE_FILE_NAME);
        try {
            fos = new FileOutputStream(fileName, true);
            fd = fos.getFD();
            var f = new FileWriter(fd);
            writer = new BufferedWriter(f);

        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        seqNumGen = new SequenceNumberGenerator(dataDir, "streaming", 1);
    }

    @Override
    public void writeRecord(Record r) {
        try {
            String cdcJson = ow.writeValueAsString(generateCdcMessageForRecord(r));
            writer.write(cdcJson);
            writer.write("\n");
            LOGGER.info("Writing CDC message = {}", cdcJson);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private HashMap<String, Object> generateCdcMessageForRecord(Record r) {
        // TODO: optimize, don't create objects every time.
        long seqNum = seqNumGen.getNext();
        HashMap<String, Object> key = new HashMap<>();
        HashMap<String, Object> fields = new HashMap<>();

        for (int i = 0; i < r.keyValues.size(); i++) {
            String formattedVal = YugabyteDialectConverter.makeSqlStatementCompatible(r.keyValues.get(i));
            key.put(r.keyColumns.get(i), formattedVal);
        }

        for (int i = 0; i < r.valueValues.size(); i++) {
            String formattedVal = YugabyteDialectConverter.makeSqlStatementCompatible(r.valueValues.get(i));
            fields.put(r.valueColumns.get(i), formattedVal);
        }

        HashMap<String, Object> cdcInfo = new HashMap<>();
        cdcInfo.put("op", r.op);
        cdcInfo.put("schema_name", r.t.schemaName);
        cdcInfo.put("table_name", r.t.tableName);
        cdcInfo.put("key", key);
        cdcInfo.put("fields", fields);
        cdcInfo.put("sequence_number", seqNum);
        return cdcInfo;
    }

    @Override
    public void flush() {
        try {
            writer.flush();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            flush();
            sync();
            writer.close();
            fos.close();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void sync() {
        try {
            fd.sync();
        }
        catch (SyncFailedException e) {
            throw new RuntimeException(e);
        }
    }
}
