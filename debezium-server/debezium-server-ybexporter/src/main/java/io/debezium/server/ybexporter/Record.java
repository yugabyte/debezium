/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.ybexporter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class Record {
    public Table t;
    public String snapshot;
    public String op;
    public HashMap<String, Object> keyFields = new HashMap<>();
    public LinkedHashMap<String, Object> valueFields = new LinkedHashMap<>();

    public String getTableIdentifier() {
        return t.toString();
    }

    public ArrayList<Object> getValueFieldValues() {
        return new ArrayList<>(valueFields.values());
    }
}
