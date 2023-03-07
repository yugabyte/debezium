/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.ybexporter;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SequenceNumberGenerator {
    private static final Logger LOGGER = LoggerFactory.getLogger(SequenceNumberGenerator.class);
    String datadir;
    String name;
    int start;
    long currentSequenceNumber;
    long range;
    long borrowedSequenceNumbersEndRange;
    String filename;

    public SequenceNumberGenerator(String datadir, String name, int start, long range) {
        this.datadir = datadir;
        this.name = name;
        this.start = start;
        if (range < 1) {
            throw new RuntimeException("Range too small");
        }
        this.range = range;
        this.currentSequenceNumber = start;
        this.borrowedSequenceNumbersEndRange = start - 1; // to start with we haven't borrowed any.
        this.filename = datadir + "/" + name + ".dat";
        restoreStateFromDisk();
    }

    private void borrowIfRequired() {
        if (currentSequenceNumber > borrowedSequenceNumbersEndRange) {
            borrowedSequenceNumbersEndRange += range;
            persistStateToDisk();
            LOGGER.info("borrowed till {}", borrowedSequenceNumbersEndRange);
        }
    }

    private void persistStateToDisk() {
        try {
            FileOutputStream fos = new FileOutputStream(filename);
            fos.write(String.valueOf(borrowedSequenceNumbersEndRange).getBytes());
            fos.getFD().sync();
            fos.close();
        }
        catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void restoreStateFromDisk() {
        try {
            String content = new String(Files.readAllBytes(Paths.get(filename)));
            borrowedSequenceNumbersEndRange = Long.parseLong(content);
            currentSequenceNumber = borrowedSequenceNumbersEndRange + 1; // start from next set.
        }
        catch (NoSuchFileException | FileNotFoundException e) {
            LOGGER.info("File does not exist. No state to read");
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public long getNext() {
        borrowIfRequired();
        return currentSequenceNumber++;
    }
}
