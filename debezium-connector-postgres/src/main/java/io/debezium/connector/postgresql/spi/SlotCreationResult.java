/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.spi;

import io.debezium.common.annotation.Incubating;
import io.debezium.connector.postgresql.connection.Lsn;

/**
 * A simple data container representing the creation of a newly created replication slot.
 */
@Incubating
public class SlotCreationResult {

    private final String slotName;
    private final Lsn walStartLsn;
    private final String snapshotName;
    private final String pluginName;
    private final boolean exportSnapshotUsed;

    public SlotCreationResult(String name, String startLsn, String snapshotName, String pluginName, boolean exportSnapshotUsed) {
        this.slotName = name;
        this.walStartLsn = Lsn.valueOf(startLsn);
        this.snapshotName = snapshotName;
        this.pluginName = pluginName;
        this.exportSnapshotUsed = exportSnapshotUsed;
    }

    /**
     * return the name of the created slot.
     */
    public String slotName() {
        return slotName;
    }

    public Lsn startLsn() {
        return walStartLsn;
    }

    public String snapshotName() {
        return snapshotName;
    }

    public String pluginName() {
        return pluginName;
    }

    public boolean isExportSnapshotUsed() {
        return exportSnapshotUsed;
    }
}
