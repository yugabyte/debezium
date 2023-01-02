/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.file;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

public class CDCWriterJson implements RecordWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableSnapshotWriterCSV.class);
    private final String dataDir;

    private BufferedWriter writer;

    ObjectWriter ow;

    public CDCWriterJson(String datadirStr) {
        dataDir = datadirStr;

        var fileName = dataDir + "/queue.json";
        try {
            var f = new FileWriter(fileName, true);
            writer = new BufferedWriter(f);

        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
    }

    @Override
    public void writeRecord(Record r) {
        try {
            String cdcJson = ow.writeValueAsString(r.getCDCInfo());
            writer.write(cdcJson);
            writer.write("\n");
            LOGGER.info("XXX CDC json = {}", cdcJson);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void flush() {
        try {
            writer.flush();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            writer.close();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
