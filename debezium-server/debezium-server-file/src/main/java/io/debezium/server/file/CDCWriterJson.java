/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.file;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

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
            // TODO: move cdcinfo generation to this class
            String cdcJson = ow.writeValueAsString(generateCdcMessageForRecord(r));
            writer.write(cdcJson);
            writer.write("\n");
            LOGGER.info("XXX CDC json = {}", cdcJson);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private HashMap<String, Object> generateCdcMessageForRecord(Record r) {
        HashMap<String, Object> key = new HashMap<>();
        HashMap<String, Object> fields = new HashMap<>();

        for (var entry : r.key.entrySet()) {
            String formattedVal = YugabyteDialectConverter.transformToSQLStatementFriendlyObject(entry.getValue());
            key.put(entry.getKey(), formattedVal);
        }

        for (var entry : r.fields.entrySet()) {
            String formattedVal = YugabyteDialectConverter.transformToSQLStatementFriendlyObject(entry.getValue());
            fields.put(entry.getKey(), formattedVal);
        }

        HashMap<String, Object> cdcInfo = new HashMap<>();
        cdcInfo.put("op", r.op);
        cdcInfo.put("schema_name", r.t.schemaName);
        cdcInfo.put("table_name", r.t.tableName);
        cdcInfo.put("key", key);
        cdcInfo.put("fields", fields);
        return cdcInfo;
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
