/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.ybexporter;

import java.io.File;
import java.io.IOException;
import java.io.SyncFailedException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingWriterJson implements RecordWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableSnapshotWriterCSV.class);
    private static final String QUEUE_FILE_NAME = "queue.json";
    private static final String QUEUE_FILE_DIR = "cdc";
    private String dataDir;
//    private RotatingFileWriter writer;
//    private BufferedWriter writer;
//    private RotatingFileWriter rfwriter;
//    private ObjectWriter ow;
//    private FileOutputStream fos;
//    private FileDescriptor fd;
    private QueueSegment currentQueueSegment;
    private long currentQueueSegmentIndex = 0;
    private static final long QUEUE_SEGMENT_MAX_BYTES = 500;

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
//        writer = new RotatingFileWriter(fileName, 500, new RotatingFileWriterCallback());
//        writer = new BufferedWriter(rfwriter);

//        ow = new ObjectMapper().writer();
        currentQueueSegment = new QueueSegment(getFilePathWithIndex(currentQueueSegmentIndex));
    }

    private String getFilePathWithIndex(long index){
        return String.format("%s.%d", QUEUE_FILE_NAME, index);
    }

    private boolean shouldRotateQueueSegment(){
        return (currentQueueSegment.getByteCount() >= QUEUE_SEGMENT_MAX_BYTES);
    }

    private void rotateQueueSegment(){
        // close old file.
        try {
            currentQueueSegment.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        currentQueueSegmentIndex++;
        currentQueueSegment = new QueueSegment(getFilePathWithIndex(currentQueueSegmentIndex));
    }

//    class RotatingFileWriterCallback implements RotatingFileCallback{
//        @Override
//        public void preRotate(Writer underlyingWriter, int currentIndex, long currentByteCount) {
//            LOGGER.info("Rotating queue file #{}", currentIndex);
//            String eofMarker = "\\.";
//            try {
//                underlyingWriter.write(eofMarker);
//                writer.flush();
//                writer.sync();
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
//        }
//    }

    @Override
    public void writeRecord(Record r) {
        if (shouldRotateQueueSegment()) rotateQueueSegment();
        currentQueueSegment.write(r);
    }

//    private HashMap<String, Object> generateCdcMessageForRecord(Record r) {
//        // TODO: optimize, don't create objects every time.
//        HashMap<String, Object> key = new HashMap<>();
//        HashMap<String, Object> fields = new HashMap<>();
//
//        for (int i = 0; i < r.keyValues.size(); i++) {
//            String formattedVal = YugabyteDialectConverter.makeSqlStatementCompatible(r.keyValues.get(i));
//            key.put(r.keyColumns.get(i), formattedVal);
//        }
//
//        for (int i = 0; i < r.valueValues.size(); i++) {
//            String formattedVal = YugabyteDialectConverter.makeSqlStatementCompatible(r.valueValues.get(i));
//            fields.put(r.valueColumns.get(i), formattedVal);
//        }
//
//        HashMap<String, Object> cdcInfo = new HashMap<>();
//        cdcInfo.put("op", r.op);
//        cdcInfo.put("schema_name", r.t.schemaName);
//        cdcInfo.put("table_name", r.t.tableName);
//        cdcInfo.put("key", key);
//        cdcInfo.put("fields", fields);
//        return cdcInfo;
//    }

    @Override
    public void flush() {
        try {
            currentQueueSegment.flush();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            currentQueueSegment.close();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void sync() {
        try {
            currentQueueSegment.sync();
        }
        catch (SyncFailedException e) {
            throw new RuntimeException(e);
        }
    }
}
