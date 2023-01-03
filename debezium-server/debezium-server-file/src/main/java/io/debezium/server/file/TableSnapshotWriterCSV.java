/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.file;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableSnapshotWriterCSV implements RecordWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableSnapshotWriterCSV.class);
    ExportStatus es;
    String dataDir;
    Table t;
    CSVPrinter csvPrinter;

    public TableSnapshotWriterCSV(String datadirStr, Table tbl) {
        dataDir = datadirStr;
        t = tbl;

        var fileName = getFullFileNameForTable();
        try {
            var f = new FileWriter(fileName);
            csvPrinter = new CSVPrinter(f, CSVFormat.POSTGRESQL_CSV);
            ArrayList<String> cols = t.getColumns();
            String header = String.join(CSVFormat.POSTGRESQL_CSV.getDelimiterString(), cols) + CSVFormat.POSTGRESQL_CSV.getRecordSeparator();
            LOGGER.info("header = {}", header);
            f.write(header);
            es = ExportStatus.getInstance(dataDir);
            es.updateTableSnapshotWriterCreated(tbl, getFilenameForTable());

        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeRecord(Record r) {
        // TODO: assert r.table = table
        try {
            csvPrinter.printRecord(r.getFieldValues());
            es.updateTableSnapshotRecordWritten(t);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String getFilenameForTable() {
        return t.tableName + "_data.sql";
    }

    private String getFullFileNameForTable() {
        return dataDir + "/" + getFilenameForTable();
    }

    @Override
    public void flush() {
        try {
            csvPrinter.flush();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            String eof = "\\.";
            csvPrinter.getOut().append(eof);
            csvPrinter.println();
            csvPrinter.println();
            csvPrinter.close(true);
            LOGGER.info("Closing snapshot file = {}", csvPrinter.getOut().toString());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
