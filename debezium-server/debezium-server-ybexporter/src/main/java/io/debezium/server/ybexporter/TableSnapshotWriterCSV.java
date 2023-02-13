/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.ybexporter;

import java.io.BufferedWriter;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.SyncFailedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.siegmar.fastcsv.writer.CsvWriter;
import de.siegmar.fastcsv.writer.LineDelimiter;
import de.siegmar.fastcsv.writer.QuoteStrategy;

public class TableSnapshotWriterCSV implements RecordWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableSnapshotWriterCSV.class);
    private ExportStatus es;
    private String dataDir;
    private Table t;
    // private CSVPrinter csvPrinter;
    private CsvWriter csvWriter;
    BufferedWriter bufferedWriter;
    private FileOutputStream fos;
    private FileDescriptor fd;

    public TableSnapshotWriterCSV(String datadirStr, Table tbl) {
        dataDir = datadirStr;
        t = tbl;

        var fileName = getFullFileNameForTable();
        try {
            fos = new FileOutputStream(fileName);
            fd = fos.getFD();
            var f = new FileWriter(fd);
            bufferedWriter = new BufferedWriter(f);

            csvWriter = CsvWriter.builder()
                    .fieldSeparator(',')
                    .quoteCharacter('"')
                    .quoteStrategy(QuoteStrategy.REQUIRED)
                    .lineDelimiter(LineDelimiter.LF)
                    .build(bufferedWriter);

            // CSVFormat fmt = CSVFormat.POSTGRESQL_CSV;
            // csvPrinter = new CSVPrinter(bufferedWriter, fmt);
            ArrayList<String> cols = t.getColumns();
            // String header = String.join(fmt.getDelimiterString(), cols) + fmt.getRecordSeparator();
            // LOGGER.debug("header = {}", header);
            // f.write(header);
            csvWriter.writeRow(cols);

            es = ExportStatus.getInstance(dataDir);
            es.updateTableSnapshotWriterCreated(tbl, getFilenameForTable());

        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void writeRecord(Record r) {
        // TODO: assert r.table = table
        try {
            // csvPrinter.printRecord(r.getValueFieldValues());
            ArrayList<Object> rowValues = r.getValueFieldValues();
            List<String> rowValuesStrings = rowValues.stream()
                    .map(object -> Objects.toString(object, null))
                    .collect(Collectors.toList());

            csvWriter.writeRow(rowValuesStrings);
            es.updateTableSnapshotRecordWritten(t);
        }
        catch (Exception e) {
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
            // csvPrinter.flush();
            bufferedWriter.flush();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            String eof = "\\.";
            // csvPrinter.getOut().append(eof);
            // csvPrinter.println();
            // csvPrinter.println();
            bufferedWriter.append(eof);
            bufferedWriter.append("\n");
            bufferedWriter.append("\n");

            flush();
            sync();
            // csvPrinter.close(true);
            csvWriter.close();
            fos.close();
            LOGGER.info("Closing snapshot file for table {}", t);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void sync() {
        try {
            fd.sync();
        }
        catch (SyncFailedException e) {
            throw new RuntimeException(e);
        }
    }
}
