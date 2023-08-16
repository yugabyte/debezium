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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import org.apache.kafka.connect.data.Field;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.graalvm.collections.Pair;
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
    private String metadataDBPath;
    private Connection metadataDBConn;
    private static String QUEUE_SEGMENT_META_TABLE_NAME = "queue_segment_meta";
    private static String EVENT_STATS_TABLE_NAME = "exported_events_stats";
    private static String EVENT_STATS_PER_TABLE_TABLE_NAME = "exported_events_stats_per_table";


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

        // open connection to metadataDB
        // TODO: interpret config vars once and make them globally available to all classes
        final Config config = ConfigProvider.getConfig();
        metadataDBPath = config.getValue("debezium.sink.ybexporter.metadata.db.path", String.class);
        if (metadataDBPath == null){
            throw new RuntimeException("please provide value for debezium.sink.ybexporter.metadata.db.path.");
        }
        metadataDBConn = null;
        try {
            String url = "jdbc:sqlite:" + metadataDBPath;
            metadataDBConn = DriverManager.getConnection(url);
            LOGGER.info("Connected to metadata db at {}", metadataDBPath);
        } catch (SQLException e) {
            throw new RuntimeException(String.format("Couldn't connect to metadata DB at %s", metadataDBPath), e);
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

        HashMap<String, Object> tableSchema = new HashMap<>();
        ArrayList<Field> fields = new ArrayList<>(t.fieldSchemas.values());
        tableSchema.put("columns", fields);
        try {
            String fileName = t.tableName;
            if ((sourceType.equals("postgresql")) && (!t.schemaName.equals("public"))){
                fileName = t.schemaName + "." + fileName;
            }
            String schemaFilePath = String.format("%s/schemas/%s_schema.json", dataDir, fileName);
            File schemaFile = new File(schemaFilePath);
            schemaWriter.writeValue(schemaFile, tableSchema);
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

    // TODO: refactor to retrieve config from a static class instead of having to set/pass it to each class.
    public void setSourceType(String sourceType) {
        this.sourceType = sourceType;
    }

    public void updateQueueSegmentCommitted(long segmentNo, long committedSize, Map<Pair<String, String>, Map<String, Long>> eventCountDeltaPerTable){
        Statement updateStmt;
        try {
            updateStmt = metadataDBConn.createStatement();
            int updatedRows = updateStmt.executeUpdate(String.format("UPDATE %s SET size_committed = %d WHERE segment_no=%d", QUEUE_SEGMENT_META_TABLE_NAME, committedSize, segmentNo));
            if (updatedRows != 1){
                throw new RuntimeException(String.format("Update of queue segment metadata failed with query-%s, rowsAffected -%d", updateStmt, updatedRows));
            }
            updateStmt.close();
        } catch (SQLException e) {
            throw new RuntimeException(String.format("Failed to run update queue segment size " +
                    "- segmentNo: %d, committedSize:%d", segmentNo, committedSize), e);
        }
    }

    public void queueSegmentCreated(long segmentNo, String segmentPath){
        Statement insertStmt;
        try {
            insertStmt = metadataDBConn.createStatement();
            insertStmt.executeUpdate(String.format("INSERT OR IGNORE into %s VALUES(%d, '%s', 0)", QUEUE_SEGMENT_META_TABLE_NAME, segmentNo, segmentPath));
            insertStmt.close();
        } catch (SQLException e) {
            throw new RuntimeException(String.format("Failed to run update queue segment size " +
                    "- segmentNo: %d", segmentNo), e);
        }
    }

    public long getQueueSegmentCommittedSize(long segmentNo){
        Statement selectStmt;
        long sizeCommitted;
        try {
            selectStmt = metadataDBConn.createStatement();
            ResultSet rs = selectStmt.executeQuery(String.format("SELECT size_committed from %s where segment_no=%s", QUEUE_SEGMENT_META_TABLE_NAME, segmentNo));
            if (!rs.next()){
                throw new RuntimeException(String.format("Could not fetch committedSize for queue segment - %d", segmentNo));
            }
            sizeCommitted = rs.getLong("size_committed");
            selectStmt.close();
        } catch (SQLException e) {
            throw new RuntimeException(String.format("Failed to run update queue segment size " +
                    "- segmentNo: %d", segmentNo), e);
        }
        return sizeCommitted;
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