/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.ybexporter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

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
        this.filename = datadir + "/" + name;
        readStateFromDisk();
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
            FileWriter fw = new FileWriter(fos.getFD());
            fw.write(String.valueOf(borrowedSequenceNumbersEndRange));
            fos.getFD().sync();
            fw.close();
        }
        catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void readStateFromDisk() {
        try {
            File file = new File(filename);
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String borrowedSequenceNumbersEndRangeStr = br.readLine();
            borrowedSequenceNumbersEndRange = Long.parseLong(borrowedSequenceNumbersEndRangeStr);
            currentSequenceNumber = borrowedSequenceNumbersEndRange + 1; // start from next set.

        }
        catch (FileNotFoundException e) {
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
