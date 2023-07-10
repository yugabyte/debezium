/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.ybexporter;

import java.io.File;
import java.io.IOException;
import java.io.SyncFailedException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Math.max;

public class StreamingWriterJson implements RecordWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamingWriterJson.class);
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
    private SequenceNumberGenerator sng;

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
        sng = new SequenceNumberGenerator(1);
        recoverStateFromDisk();
        if (currentQueueSegment == null){
            createNewQueueSegment();
        }

    }

    private void createNewQueueSegment(){
        currentQueueSegment = new QueueSegment(getFilePathWithIndex(currentQueueSegmentIndex));
    }

    private void recoverStateFromDisk(){
        recoverLatestQueueSegment();
        // recover sequence numberof last written record and resume
        if (currentQueueSegment != null){
            sng.advanceTo(currentQueueSegment.getSequenceNumberOfLastRecord() + 1);
        }
    }

    private void recoverLatestQueueSegment(){
        // read dir to find all queue files
        Path queueDirPath = Path.of(dataDir, QUEUE_FILE_DIR);
        String searchGlob = String.format("%s.*", QUEUE_FILE_NAME);
        ArrayList<Path> filePaths = new ArrayList<>();
        try {
            DirectoryStream<Path> stream = Files.newDirectoryStream(queueDirPath, searchGlob);
            for (Path entry: stream) {
                filePaths.add(entry);
            }

            if (filePaths.size() == 0){
                // no files found. nothing to recover.
                LOGGER.info("No files found matching {}. Nothing to recover", searchGlob);
                return;
            }
            // extract max index of all files
            int maxIndex = 0;
            for(Path p: filePaths){
                // get the substring after the last occurence of "." and convert to ind
                int index = Integer.parseInt(p.toString().substring(p.toString().lastIndexOf('.') + 1));
                maxIndex = max(maxIndex, index);
            }
            // create file writer for last file segment
            currentQueueSegmentIndex = maxIndex;
            createNewQueueSegment();

            LOGGER.info("Recovered from queue segment-{} with byte count={}", getFilePathWithIndex(currentQueueSegmentIndex), currentQueueSegment.getByteCount());
        }
        catch (IOException x) {
            throw new RuntimeException(x);
        }
    }

    private String getFilePathWithIndex(long index){
        String queueSegmentFileName = String.format("%s.%d", QUEUE_FILE_NAME, index);
        return Path.of(dataDir, QUEUE_FILE_DIR, queueSegmentFileName).toString();
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
        createNewQueueSegment();
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
        augmentRecordWithSequenceNo(r);
        currentQueueSegment.write(r);
    }

    private void augmentRecordWithSequenceNo(Record r){
        r.vsn = sng.getNextValue();
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

class SequenceNumberGenerator{
    long currentValue;
    public SequenceNumberGenerator(long start){
        currentValue = start;
    }

    public long getNextValue(){
        return currentValue++;
    }

    public void advanceTo(long val){
        currentValue = val;
    }
}
