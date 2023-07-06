/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.ybexporter;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.SyncFailedException;
import java.io.Writer;

public class RotatingFileWriter extends Writer {
    private String path;
    private int maxFileByteCount;
    private RotatingFileCallback callback;
    private int currentIndex;
    private FileWriter currentFileWriter;
    private long currentFileByteCount;
    private FileDescriptor currentFileFd;
    private FileOutputStream currentFileOutputStream;

    public RotatingFileWriter(String path, int maxFileByteCount, RotatingFileCallback callback){
        this.path = path;
        this.maxFileByteCount = maxFileByteCount;
        this.callback = callback;
        currentIndex = 0;
        createNewFileWriter();
    }

    private String getFilePathWithIndex(int index){
        return String.format("%s.%d", path, index);
    }

    public int getCurrentIndex(){
        return currentIndex;
    }

    private void createNewFileWriter(){
        try {
            // close current file
            if (currentFileWriter != null){
                currentFileWriter.flush();
                currentFileWriter.close();
            }

            // open new file
            currentFileOutputStream = new FileOutputStream(getFilePathWithIndex(currentIndex), true);
            currentFileFd = currentFileOutputStream.getFD();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        currentFileWriter = new FileWriter(currentFileFd);
        currentFileByteCount = 0; // TODO retrieve from file if already exists.
    }

    private void rotate(){
        if (callback != null){
            callback.preRotate(currentFileWriter, currentIndex, currentFileByteCount);
        }
        currentIndex++;
        createNewFileWriter();
    }

    @Override
    public void write(char[] cbuf, int off, int len) throws IOException {
        if (currentFileByteCount >= maxFileByteCount){
            rotate();
        }
        currentFileWriter.write(cbuf, off, len);
        currentFileByteCount += len;
    }

    @Override
    public void flush() throws IOException {
        currentFileWriter.flush();
    }

    @Override
    public void close() throws IOException {
        currentFileOutputStream.close();
        currentFileWriter.close();
    }

    public void sync() throws SyncFailedException {
        currentFileFd.sync();
    }
}


