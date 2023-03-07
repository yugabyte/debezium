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

/**
 * Generates sequence numbers with a requirement that they are always in increasing order,
 * but may not be contiguous.
 * The generator is resistant to process crashes. This is achieved by simulating
 * a process of borrowing sequence numbers. The generator effectively borrows numbers before
 * it can give them out, and the last borrowed sequence number is persisted to disk. In the event of a restart,
 * it restores the state from disk (based on the name given), and starts giving out sequence numbers
 * from the next borrowed set.
 */
public class SequenceNumberGenerator {
    private static final Logger LOGGER = LoggerFactory.getLogger(SequenceNumberGenerator.class);
    private String datadir;
    private String name;
    private int start;
    private long currentSequenceNumber;
    private long borrowSize = 100000;
    private long borrowedSequenceNumbersEndRange;
    private String filename;

    public SequenceNumberGenerator(String datadir, String name, int start) {
        this.datadir = datadir;
        this.name = name;
        this.start = start;
        this.currentSequenceNumber = start;
        this.borrowedSequenceNumbersEndRange = start - 1; // to start with we haven't borrowed any.
        this.filename = datadir + "/" + name + ".dat";
        restoreStateFromDisk();
    }

    private void borrowIfRequired() {
        if (currentSequenceNumber > borrowedSequenceNumbersEndRange) {
            borrowedSequenceNumbersEndRange += borrowSize;
            persistStateToDisk();
            LOGGER.debug("Borrowed sequence numbers till {}", borrowedSequenceNumbersEndRange);
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
