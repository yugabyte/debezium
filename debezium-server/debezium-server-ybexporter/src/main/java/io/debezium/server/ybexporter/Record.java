/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.ybexporter;

import java.util.ArrayList;

public class Record {
    public Table t;
    public String snapshot;
    public String op;
    // public HashMap<String, Object> keyFields = new HashMap<>();
    // public LinkedHashMap<String, Object> valueFields = new LinkedHashMap<>();

    public ArrayList<String> keyColumns = new ArrayList<>();
    public ArrayList<Object> keyValues = new ArrayList<>();
    public ArrayList<String> valueColumns = new ArrayList<>();
    public ArrayList<Object> valueValues = new ArrayList<>();

    public void clear() {
        t = null;
        snapshot = null;
        op = null;
        keyColumns.clear();
        keyValues.clear();
        valueColumns.clear();
        valueValues.clear();
    }

    public String getTableIdentifier() {
        return t.toString();
    }

    public ArrayList<Object> getValueFieldValues() {
        return valueValues;
        // return new ArrayList<>(valueFields.values());
    }

    public void addValueField(String key, Object value) {
        valueColumns.add(key);
        valueValues.add(value);
    }

    public void addKeyField(String key, Object value) {
        keyColumns.add(key);
        keyValues.add(value);
    }
}
