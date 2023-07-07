/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.ybexporter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.SyncFailedException;
import java.io.Writer;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

public class StreamingWriterJson implements RecordWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableSnapshotWriterCSV.class);
    private static final String QUEUE_FILE_NAME = "queue.json";
    private static final String QUEUE_FILE_DIR = "cdc";
    private String dataDir;
    private RotatingFileWriter writer;
//    private BufferedWriter writer;
//    private RotatingFileWriter rfwriter;
    private ObjectWriter ow;
//    private FileOutputStream fos;
//    private FileDescriptor fd;

    public StreamingWriterJson(String datadirStr) {
        dataDir = datadirStr;

        var fileName = String.format("%s/%s/%s", dataDir, QUEUE_FILE_DIR, QUEUE_FILE_NAME);
        // mkdir cdc
        File queueDir = new File(String.format("%s/%s", dataDir, QUEUE_FILE_DIR));
        if (!queueDir.exists()){
            boolean dirCreated = queueDir.mkdir();
            if (!dirCreated){
                throw new RuntimeException("failed to create dir for cdc");
            }
        }
        //            fos = new FileOutputStream(fileName, true);
//            fd = fos.getFD();
//            var f = new FileWriter(fd);
        writer = new RotatingFileWriter(fileName, 500, new RotatingFileWriterCallback());
//        writer = new BufferedWriter(rfwriter);

        ow = new ObjectMapper().writer();
    }

    class RotatingFileWriterCallback implements RotatingFileCallback{
        @Override
        public void preRotate(Writer underlyingWriter, int currentIndex, long currentByteCount) {
            LOGGER.info("Rotating queue file #{}", currentIndex);
            String eofMarker = "\\.";
            try {
                underlyingWriter.write(eofMarker);
                writer.flush();
                writer.sync();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void writeRecord(Record r) {
        try {
            String cdcJson = ow.writeValueAsString(generateCdcMessageForRecord(r)) + "\n";
            writer.write(cdcJson);
            LOGGER.info("Writing CDC message = {}", cdcJson);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private HashMap<String, Object> generateCdcMessageForRecord(Record r) {
        // TODO: optimize, don't create objects every time.
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
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void sync() {
        try {
            writer.sync();
        }
        catch (SyncFailedException e) {
            throw new RuntimeException(e);
        }
    }
}
