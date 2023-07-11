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
    private static final String QUEUE_FILE_NAME = "queue";
    private static final String QUEUE_FILE_EXTENSION = "ndjson";
    private static final String QUEUE_FILE_DIR = "cdc";
    private static final long QUEUE_SEGMENT_MAX_BYTES = 500;
    private String dataDir;
    private QueueSegment currentQueueSegment;
    private long currentQueueSegmentIndex = 0;


    public StreamingWriterJson(String datadirStr) {
        dataDir = datadirStr;

        // mkdir cdc
        File queueDir = new File(String.format("%s/%s", dataDir, QUEUE_FILE_DIR));
        if (!queueDir.exists()){
            boolean dirCreated = queueDir.mkdir();
            if (!dirCreated){
                throw new RuntimeException("failed to create dir for cdc");
            }
        }
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
    }

    private void recoverLatestQueueSegment(){
        // read dir to find all queue files
        Path queueDirPath = Path.of(dataDir, QUEUE_FILE_DIR);
        String searchGlob = String.format("%s.*.%s", QUEUE_FILE_NAME, QUEUE_FILE_EXTENSION);
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
                String pathWithoutExtention = p.toString().replace("."+QUEUE_FILE_EXTENSION, "");
                int index = Integer.parseInt(pathWithoutExtention.substring(pathWithoutExtention.lastIndexOf('.') + 1));
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
        String queueSegmentFileName = String.format("%s.%d.%s", QUEUE_FILE_NAME, index, QUEUE_FILE_EXTENSION);
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

    @Override
    public void writeRecord(Record r) {
        if (shouldRotateQueueSegment()) rotateQueueSegment();
        currentQueueSegment.write(r);
    }

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
