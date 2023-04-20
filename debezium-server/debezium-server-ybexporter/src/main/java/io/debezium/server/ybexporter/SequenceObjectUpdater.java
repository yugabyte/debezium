/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.ybexporter;

import org.eclipse.microprofile.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class SequenceObjectUpdater {
    private static final Logger LOGGER = LoggerFactory.getLogger(SequenceObjectUpdater.class);
    String dataDirStr;
    Map<String, Map<String, Map<String, String>>> columnSequenceMap; // Schema:table:column -> sequence
    Map<String, Long> sequenceMax;
    ExportStatus es;
    public SequenceObjectUpdater(String dataDirStr, String columnSequenceMapString, Map<String, Long> sequenceMax){
        this.dataDirStr = dataDirStr;
        this.columnSequenceMap = new HashMap<>();

        es = ExportStatus.getInstance(dataDirStr);
        if (sequenceMax == null){
           this.sequenceMax = new HashMap<>();
        }
        else{
            this.sequenceMax = sequenceMax;
        }
        initColumnSequenceMap(columnSequenceMapString);
        es.setSequenceMaxMap(this.sequenceMax);
    }

    public void initColumnSequenceMap(String columnSequenceMapString){
        String[] columnSequenceItems = columnSequenceMapString.split(",");
        for (String columnSequenceStr: columnSequenceItems) {
            String[] colSequence = columnSequenceStr.split(":");
            if (colSequence.length != 2) {
                throw new RuntimeException("Incorrect config. Please provide comma separated list of " +
                        "'col:seq' with their fully qualified names.");
            }
            String fullyQualifiedColumnName = colSequence[0];
            String sequenceName = colSequence[1];

            String columnSplit[] = fullyQualifiedColumnName.split("\\.");
            if (columnSplit.length != 3){
                throw new RuntimeException("Incorrect format for column name in config param -" + columnSequenceStr +
                        ". Use <schema>.<table>.<column> instead.");
            }
            insertIntoColumnSequenceMap(columnSplit[0], columnSplit[1], columnSplit[2], sequenceName);
            // add a base entry to sequenceMax. If already populated, do not update.
            this.sequenceMax.put(sequenceName, this.sequenceMax.getOrDefault(sequenceName, (long)0));
        }
    }

    private void insertIntoColumnSequenceMap(String schema, String table, String column, String sequence){
        Map<String, Map<String, String>> schemaMap = this.columnSequenceMap.get(schema);
        if (schemaMap == null){
            schemaMap = new HashMap<>();
            this.columnSequenceMap.put(schema, schemaMap);
        }

        Map<String, String> schemaTableMap = schemaMap.get(table);
        if (schemaTableMap == null){
            schemaTableMap = new HashMap<>();
            schemaMap.put(table, schemaTableMap);
        }
        schemaTableMap.put(column, sequence);
    }

    private String getFromColumnSequenceMap(String schema, String table, String column){
        Map<String, Map<String, String>> schemaMap = this.columnSequenceMap.get(schema);
        if (schemaMap == null){
            return null;
        }

        Map<String, String> schemaTableMap = schemaMap.get(table);
        if (schemaTableMap == null){
            return null;
        }
        return schemaTableMap.get(column);
    }

    public void processRecord(Record r){
        if (r.op.equals("d")){
            // not processing delete events because the max would have already been updated in the
            // prior create/update events.
            // TODO: question: should we also update the max to the second-max value if the row with max is deleted??
            return;
        }
        for(int i = 0; i < r.valueColumns.size(); i++){
//            String fullyQualifiedColumnName = r.t.schemaName+"."+r.t.tableName+"."+r.valueColumns.get(i);
            String seqName = getFromColumnSequenceMap(r.t.schemaName, r.t.tableName, r.valueColumns.get(i));
            if (seqName != null){
//                String seqName = columnSequenceMap.get(fullyQualifiedColumnName);
                Long columnValue = Long.valueOf(r.valueValues.get(i).toString());
                sequenceMax.put(seqName, Math.max(sequenceMax.get(seqName), columnValue));
            }
        }

    }

}
