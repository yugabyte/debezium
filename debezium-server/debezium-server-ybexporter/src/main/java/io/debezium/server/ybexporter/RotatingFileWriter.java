/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.ybexporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.SyncFailedException;
import java.io.Writer;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

import static java.lang.Math.max;

public class RotatingFileWriter extends Writer {
    private static final Logger LOGGER = LoggerFactory.getLogger(RotatingFileWriter.class);
    private String path;
    private int maxFileByteCount;
    private RotatingFileCallback callback;
    private int currentFileSegmentIndex;
    private FileWriter currentFileSegmentWriter;
    private long currentFileSegmentByteCount;
    private FileDescriptor currentFileSegmentFd;
    private FileOutputStream currentFileSegmentOutputStream;

    public RotatingFileWriter(String path, int maxFileByteCount, RotatingFileCallback callback){
        this.path = path;
        this.maxFileByteCount = maxFileByteCount;
        this.callback = callback;
        boolean recovered = recoverStateFromDisk();
        if (!recovered){
            currentFileSegmentIndex = 0;
            createNewFileWriter();
        }
    }

    private String getFilePathWithIndex(int index){
        return String.format("%s.%d", path, index);
    }

    private boolean recoverStateFromDisk(){
        // read dir to find all queue files
        File f = new File(path);
        String dirPath = new File(path).getParent();
        String fileName = f.getName();
        ArrayList<Path> filePaths = new ArrayList<>();
        String searchGlob = String.format("%s.*", fileName);
        try {
            DirectoryStream<Path> stream = Files.newDirectoryStream(Path.of(dirPath), searchGlob);
            for (Path entry: stream) {
                filePaths.add(entry);
            }

            if (filePaths.size() == 0){
                // no files found. nothing to recover.
                LOGGER.info("No files found matching {}. Nothing to recover", searchGlob);
                return false;
            }
            // extract max index of all files
            int maxIndex = 0;
            for(Path p: filePaths){
                // get the substring after the last occurence of "." and convert to ind
                int index = Integer.parseInt(p.toString().substring(p.toString().lastIndexOf('.') + 1));
                maxIndex = max(maxIndex, index);
            }
            // create file writer for last file segment
            currentFileSegmentIndex = maxIndex;
            createNewFileWriter();

            // update currentFileByteCount by getting size of file.
            currentFileSegmentByteCount = Files.size(Path.of(getFilePathWithIndex(currentFileSegmentIndex)));
            LOGGER.info("Recovered from file-{} with byte count={}", getFilePathWithIndex(currentFileSegmentIndex), currentFileSegmentByteCount);
        }
        catch (IOException x) {
            throw new RuntimeException(x);
        }
        return true;
    }

    private void createNewFileWriter(){
        try {
            // close current file
            if (currentFileSegmentWriter != null){
                currentFileSegmentWriter.flush();
                currentFileSegmentWriter.close();
            }

            // open new file
            currentFileSegmentOutputStream = new FileOutputStream(getFilePathWithIndex(currentFileSegmentIndex), true);
            currentFileSegmentFd = currentFileSegmentOutputStream.getFD();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        currentFileSegmentWriter = new FileWriter(currentFileSegmentFd);
        currentFileSegmentByteCount = 0; // TODO retrieve from file if already exists.
    }

    private void rotate(){
        if (callback != null){
            callback.preRotate(currentFileSegmentWriter, currentFileSegmentIndex, currentFileSegmentByteCount);
        }
        currentFileSegmentIndex++;
        createNewFileWriter();
    }

    @Override
    public void write(char[] cbuf, int off, int len) throws IOException {
        if (currentFileSegmentByteCount >= maxFileByteCount){
            rotate();
        }
        currentFileSegmentWriter.write(cbuf, off, len);
        currentFileSegmentByteCount += len;
    }

    @Override
    public void flush() throws IOException {
        currentFileSegmentWriter.flush();
    }

    @Override
    public void close() throws IOException {
        currentFileSegmentOutputStream.close();
        currentFileSegmentWriter.close();
    }

    public void sync() throws SyncFailedException {
        currentFileSegmentFd.sync();
    }
}


