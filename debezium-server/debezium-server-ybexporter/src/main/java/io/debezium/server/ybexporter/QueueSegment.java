/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.ybexporter;


import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.graalvm.collections.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * A QueueSegment represents a segment of the cdc queue.
 */
public class QueueSegment {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueueSegment.class);

    private static final String EOF_MARKER = "\\.";
    private String filePath;
    private long segmentNo;
    private FileOutputStream fos;
    private FileDescriptor fd;
    private Writer writer;
    private long byteCount;
    private ObjectWriter ow;
    private ExportStatus es;
    private Map<Pair<String, String>, Long> insertEventCountDeltaPerTable;
    private Map<Pair<String, String>, Long> updateEventCountDeltaPerTable;
    private Map<Pair<String, String>, Long> deleteEventCountDeltaPerTable;

    public QueueSegment(String datadirStr, long segmentNo, String filePath){
        this.segmentNo = segmentNo;
        this.filePath = filePath;
        es = ExportStatus.getInstance(datadirStr);
        ow = new ObjectMapper().writer();
        try {
            openFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        es.queueSegmentCreated(segmentNo, filePath);
        long committedSize = es.getQueueSegmentCommittedSize(segmentNo);
        if (committedSize < byteCount){
            truncateFileAfterOffset(committedSize);
        }
    }

    private void openFile() throws IOException {
        fos = new FileOutputStream(filePath, true);
        fd = fos.getFD();
        FileWriter fw = new FileWriter(fd);
        writer = new BufferedWriter(fw);
        byteCount = Files.size(Path.of(filePath));
    }

    public long getByteCount() {
        return byteCount;
    }

    public void write(Record r){
        try {
            String cdcJson = ow.writeValueAsString(generateCdcMessageForRecord(r)) + "\n";
            writer.write(cdcJson);
            byteCount += cdcJson.length();
            updateStats(r);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    private void updateStats(Record r){
        Pair<String, String> fullyQualifiedTableName =  Pair.create(r.t.schemaName, r.t.tableName);
        Long currentCount;
        switch (r.op) {
            case "c":
                currentCount = insertEventCountDeltaPerTable.getOrDefault(fullyQualifiedTableName, 0L);
                insertEventCountDeltaPerTable.put(fullyQualifiedTableName, currentCount + 1);
                break;
            case "u":
                currentCount = updateEventCountDeltaPerTable.getOrDefault(fullyQualifiedTableName, 0L);
                updateEventCountDeltaPerTable.put(fullyQualifiedTableName, currentCount + 1);
                break;
            case "d":
                currentCount = deleteEventCountDeltaPerTable.getOrDefault(fullyQualifiedTableName, 0L);
                deleteEventCountDeltaPerTable.put(fullyQualifiedTableName, currentCount + 1);
                break;
        }
    }

    private HashMap<String, Object> generateCdcMessageForRecord(Record r) {
        // TODO: optimize, don't create objects every time.
        HashMap<String, Object> key = new HashMap<>();
        HashMap<String, Object> fields = new HashMap<>();

        for (int i = 0; i < r.keyValues.size(); i++) {
            Object formattedVal = r.keyValues.get(i);
            key.put(r.keyColumns.get(i), formattedVal);
        }

        for (int i = 0; i < r.valueValues.size(); i++) {
            Object formattedVal = r.valueValues.get(i);
            fields.put(r.valueColumns.get(i), formattedVal);
        }

        HashMap<String, Object> cdcInfo = new HashMap<>();
        cdcInfo.put("op", r.op);
        cdcInfo.put("vsn", r.vsn);
        cdcInfo.put("schema_name", r.t.schemaName);
        cdcInfo.put("table_name", r.t.tableName);
        cdcInfo.put("key", key);
        cdcInfo.put("fields", fields);
        return cdcInfo;
    }

    public void flush() throws IOException {
        writer.flush();
    }

    public void close() throws IOException {
        LOGGER.info("Closing queue file {}", filePath);
        writer.write(EOF_MARKER);
        writer.write("\n");
        writer.write("\n");
        writer.flush();
        sync();
        writer.close();
    }

    public void sync() throws IOException{
        fd.sync();
        // TODO: is Files.size going to be slow? Maybe just use byteCount?
        es.updateQueueSegmentCommittedSize(segmentNo, Files.size(Path.of(filePath)));
    }

    public long getSequenceNumberOfLastRecord(){
        ObjectMapper mapper = new ObjectMapper(new JsonFactory());
        long vsn = -1;
        String last = null, line;
        BufferedReader input;
        try {
            input = new BufferedReader(new FileReader(filePath));
            while ((line = input.readLine()) != null) {
                if (line.equals(EOF_MARKER)){
                    break;
                }
                last = line;
            }
            if (last != null){
                JsonNode lastRecordJson = mapper.readTree(last);
                vsn = lastRecordJson.get("vsn").asLong();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return vsn;
    }

    private void truncateFileAfterOffset(long offset){
        try {
            writer.close();
            LOGGER.info("Truncating queue segment {} at path {} to size {}", segmentNo, filePath, offset);
            RandomAccessFile f = new RandomAccessFile(filePath, "rw");
            f.setLength(offset);
            f.close();
            openFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
