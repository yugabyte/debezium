/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.ybexporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class SequenceObjectUpdater {
    private static final Logger LOGGER = LoggerFactory.getLogger(SequenceObjectUpdater.class);
    String dataDirStr;
    Map<String, String> columnSequenceMap;
    Map<String, Long> sequenceMax;
    ExportStatus es;
    public SequenceObjectUpdater(String dataDirStr, Map<String, String> columnSequenceMap, Map<String, Long> sequenceMax){
        this.dataDirStr = dataDirStr;
        this.columnSequenceMap = columnSequenceMap;
        es = ExportStatus.getInstance(dataDirStr);
        if (sequenceMax == null){
           this.sequenceMax = new HashMap<>();
            for (String seq : this.columnSequenceMap.values()) {
                this.sequenceMax.put(seq, (long) 0);
            }
        }
        else{
            this.sequenceMax = sequenceMax;
        }
        es.setSequenceMaxMap(this.sequenceMax);
    }

    public void processRecord(Record r){
        if (r.op.equals("d")){
            // not processing delete events because the max would have already been updated in the
            // prior create/update events.
            // TODO: question: should we also update the max to the second-max value if the row with max is deleted??
            return;
        }
        for(int i = 0; i < r.valueColumns.size(); i++){
            String fullyQualifiedColumnName = r.t.schemaName+"."+r.t.tableName+"."+r.valueColumns.get(i);
            if (columnSequenceMap.get(fullyQualifiedColumnName) != null){
                String seqName = columnSequenceMap.get(fullyQualifiedColumnName);
                Long columnValue = Long.valueOf(r.valueValues.get(i).toString());
                sequenceMax.put(seqName, Math.max(sequenceMax.get(seqName), columnValue));
            }
        }

    }

}
