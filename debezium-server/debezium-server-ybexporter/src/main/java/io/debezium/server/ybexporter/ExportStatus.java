/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.ybexporter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.connect.data.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

/**
 * Singleton class that is used to update the status of the export process.
 */
public class ExportStatus {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExportStatus.class);
    private static final String EXPORT_STATUS_FILE_NAME = "export_status.json";
    private static ExportStatus instance;
    private static ObjectMapper mapper = new ObjectMapper(new JsonFactory());
    private String dataDir;
    private String sourceType;
    private ConcurrentMap<String, Long> sequenceMax;
    private ConcurrentMap<Table, TableExportStatus> tableExportStatusMap = new ConcurrentHashMap<>();
    private ExportMode mode;
    private ObjectWriter ow;
    private File f;
    private File tempf;


    /**
     * Should only be called once in the lifetime of the process.
     * Creates and instance and assigns it to static instance property of the class.
     */
    private ExportStatus(String datadirStr) {
        //
        if (instance != null) {
            throw new RuntimeException("instance already exists.");
        }
        dataDir = datadirStr;
        ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        f = new File(getFilePath(datadirStr));

        // mkdir schemas
        File schemasDir = new File(String.format("%s/%s", dataDir, "schemas"));
        if (!schemasDir.exists()){
            boolean dirCreated = new File(String.format("%s/%s", dataDir, "schemas")).mkdir();
            if (!dirCreated){
                throw new RuntimeException("failed to create dir for schemas");
            }
        }
        instance = this;
    }

    /**
     * 1. Tries to check if an instance is already created
     * 2. If not, tries to load from disk.
     * 3. If not available on disk, creates a new instance.
     * @param datadirStr - data directory
     * @return instance
     */
    public static ExportStatus getInstance(String datadirStr) {
        if (instance != null) {
            return instance;
        }
        ExportStatus instanceFromDisk = loadFromDisk(datadirStr);
        if (instanceFromDisk != null) {
            instance = instanceFromDisk;
            return instance;
        }
        return new ExportStatus(datadirStr);
    }

    public ExportMode getMode() {
        return mode;
    }

    public void updateTableSchema(Table t){
        ObjectMapper schemaMapper = new ObjectMapper();
        schemaMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        ObjectWriter schemaWriter = schemaMapper.writer().withDefaultPrettyPrinter();

        HashMap<String, Field> tableSchema = new HashMap<>();
        for (Map.Entry<String, Field> entry : t.fieldSchemas.entrySet()) {
            String fieldName = entry.getKey();
            Field field = entry.getValue();
            tableSchema.put(fieldName, field);
        }
        try {
            String fileName = t.tableName;
            if ((sourceType.equals("postgresql")) && (!t.schemaName.equals("public"))){
                fileName = t.schemaName + "." + fileName;
            }
            String schemaFilePath = String.format("%s/schemas/%s_schema.json", dataDir, fileName);
            File schemaFile = new File(schemaFilePath);
            schemaWriter.writeValue(schemaFile, tableSchema);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void updateTableSnapshotWriterCreated(Table t, String tblFilename) {
        TableExportStatus tableExportStatus = new TableExportStatus(tableExportStatusMap.size(), tblFilename);
        tableExportStatusMap.put(t, tableExportStatus);
    }

    public void updateTableSnapshotRecordWritten(Table t) {
        tableExportStatusMap.get(t).exportedRowCountSnapshot++;
    }

    public void updateMode(ExportMode modeEnum) {
        mode = modeEnum;
    }

    public void setSequenceMaxMap(ConcurrentMap<String, Long> sequenceMax){
        this.sequenceMax = sequenceMax;
    }

    public ConcurrentMap<String, Long> getSequenceMaxMap(){
        return this.sequenceMax;
    }

    public synchronized void flushToDisk() {
        // TODO: do not create fresh objects every time, just reuse.
        HashMap<String, Object> exportStatusMap = new HashMap<>();
        List<HashMap<String, Object>> tablesInfo = new ArrayList<>();
        for (Map.Entry<Table, TableExportStatus> pair : tableExportStatusMap.entrySet()) {
            Table t = pair.getKey();
            TableExportStatus tes = pair.getValue();
            HashMap<String, Object> tableInfo = new HashMap<>();
            tableInfo.put("database_name", t.dbName);
            tableInfo.put("schema_name", t.schemaName);
            tableInfo.put("table_name", t.tableName);
            tableInfo.put("file_name", tes.snapshotFilename);
            tableInfo.put("exported_row_count_snapshot", tes.exportedRowCountSnapshot);
            tableInfo.put("sno", tes.sno);
            tablesInfo.add(tableInfo);
        }

        exportStatusMap.put("source_type", sourceType);
        exportStatusMap.put("tables", tablesInfo);
        exportStatusMap.put("mode", mode);
        exportStatusMap.put("sequences", sequenceMax);

        try {
            // for atomic write, we write to a temp file, and then
            // rename to destination file path. This prevents readers from reading the file in a corrupted
            // state (for example, when the complete file has not been written)
            tempf = new File(getTempFilePath());
            ow.writeValue(tempf, exportStatusMap);
            Files.move(tempf.toPath(), f.toPath(), REPLACE_EXISTING);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String getFilePath(String dataDirStr){
        return String.format("%s/%s", dataDirStr, EXPORT_STATUS_FILE_NAME);
    }

    private String getTempFilePath(){
        return getFilePath(dataDir) + ".tmp";
    }

    private static ExportStatus loadFromDisk(String datadirStr) {
        try {
            Path p = Paths.get(getFilePath(datadirStr));
            File f = new File(p.toUri());
            if (!f.exists()) {
                return null;
            }

            String fileContent = Files.readString(p);
            var exportStatusJson = mapper.readTree(fileContent);
            LOGGER.info("Loaded export status info from disk = {}", exportStatusJson);

            ExportStatus es = new ExportStatus(datadirStr);
            es.updateMode(ExportMode.valueOf(exportStatusJson.get("mode").asText()));
            es.setSourceType(exportStatusJson.get("source_type").asText());

            var tablesJson = exportStatusJson.get("tables");
            for (var tableJson : tablesJson) {
                // TODO: creating a duplicate table here. it will again be created when parsing a record of the table for the first time.
                Table t = new Table(tableJson.get("database_name").asText(), tableJson.get("schema_name").asText(), tableJson.get("table_name").asText());

                TableExportStatus tes = new TableExportStatus(tableJson.get("sno").asInt(), tableJson.get("file_name").asText());
                tes.exportedRowCountSnapshot = tableJson.get("exported_row_count_snapshot").asLong();
                es.tableExportStatusMap.put(t, tes);
            }
            var sequencesJson = exportStatusJson.get("sequences");
            var sequencesIterator = sequencesJson.fields();
            ConcurrentHashMap<String, Long> sequenceMaxMap = new ConcurrentHashMap<>();
            while (sequencesIterator.hasNext()){
                var entry = sequencesIterator.next();
                sequenceMaxMap.put(entry.getKey(), Long.valueOf(entry.getValue().asText()));
            }
            es.setSequenceMaxMap(sequenceMaxMap);

            return es;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void setSourceType(String sourceType) {
        this.sourceType = sourceType;
    }
}

class TableExportStatus {
    Integer sno;
    Long exportedRowCountSnapshot;
    String snapshotFilename;

    public TableExportStatus(Integer sno, String snapshotFilename){
        this.sno = sno;
        this.snapshotFilename = snapshotFilename;
        this.exportedRowCountSnapshot = 0L;
    }
}

enum ExportMode {
    SNAPSHOT,
    STREAMING,
}