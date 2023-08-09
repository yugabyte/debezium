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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileDescriptor;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.SyncFailedException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Objects;

/**
 * A QueueSegment represents a segment of the cdc queue.
 */
public class QueueSegment {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueueSegment.class);

    private static final String EOF_MARKER = "\\.\n\n";
    private String filePath;
    private long segmentNo;
    private FileOutputStream fos;
    private FileDescriptor fd;
    private Writer writer;
    private long byteCount;
    private ObjectWriter ow;
    private ExportStatus es;

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
        if (committedSize > 0){
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
        writer.flush();
        sync();
        writer.close();
    }

    public void sync() throws SyncFailedException, IOException{
        // flushing the buffer before we sync.
        flush();
        fd.sync();
        es.updateQueueSegmentCommittedSize(segmentNo, byteCount);
    }

    public long getSequenceNumberOfLastRecord(){
        ObjectMapper mapper = new ObjectMapper(new JsonFactory());
        long vsn = 0;
        String last = null, line;
        BufferedReader input;
        try {
            input = new BufferedReader(new FileReader(filePath));
            while ((line = input.readLine()) != null) {
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
            close();
            RandomAccessFile f = new RandomAccessFile(filePath, "rw");
            f.setLength(offset);
            f.close();
            openFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
