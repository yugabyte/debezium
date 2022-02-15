/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.yugabytedb.connection.ReplicationMessage;
import io.debezium.connector.yugabytedb.connection.YugabyteDBConnection;
import io.debezium.data.Envelope.Operation;
import io.debezium.function.Predicates;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.util.Clock;
import io.debezium.util.Strings;

/**
 * Emits change data based on a logical decoding event coming as protobuf or JSON message.
 *
 * @author Horia Chiorean (hchiorea@redhat.com), Jiri Pechanec
 */
public class YugabyteDBChangeRecordEmitter extends RelationalChangeRecordEmitter {
    private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBChangeRecordEmitter.class);

    private final ReplicationMessage message;
    private final YugabyteDBSchema schema;
    private final YugabyteDBConnectorConfig connectorConfig;
    private final YugabyteDBConnection connection;
    private final TableId tableId;

    private final String pgSchemaName;

    public YugabyteDBChangeRecordEmitter(Partition partition, OffsetContext offset, Clock clock, YugabyteDBConnectorConfig connectorConfig, YugabyteDBSchema schema,
                                         YugabyteDBConnection connection, TableId tableId,
                                         ReplicationMessage message) {
        super(partition, offset, clock);

        this.schema = schema;
        this.message = message;
        this.connectorConfig = connectorConfig;
        this.connection = connection;

        this.tableId = tableId;
        Objects.requireNonNull(this.tableId);

        this.pgSchemaName = null;
    }

    public YugabyteDBChangeRecordEmitter(Partition partition, OffsetContext offset, Clock clock, YugabyteDBConnectorConfig connectorConfig, YugabyteDBSchema schema,
                                         YugabyteDBConnection connection, TableId tableId,
                                         ReplicationMessage message, String pgSchemaName) {
        super(partition, offset, clock);

        this.schema = schema;
        this.message = message;
        this.connectorConfig = connectorConfig;
        this.connection = connection;

        this.pgSchemaName = pgSchemaName;

        this.tableId = tableId;
        Objects.requireNonNull(this.tableId);
    }

    @Override
    protected Operation getOperation() {
        switch (message.getOperation()) {
            case INSERT:
                return Operation.CREATE;
            case UPDATE:
                return Operation.UPDATE;
            case DELETE:
                return Operation.DELETE;
            case TRUNCATE:
                return Operation.TRUNCATE;
            default:
                throw new IllegalArgumentException("Received event of unexpected command type: " + message.getOperation());
        }
    }

    @Override
    public void emitChangeRecords(DataCollectionSchema schema, Receiver receiver) throws InterruptedException {
        schema = synchronizeTableSchema(schema);
        LOGGER.debug("SKSK the schema of the table is " + schema);
        LOGGER.info("VKVK the schema initialized is " + pgSchemaName);
        super.emitChangeRecords(schema, receiver);
    }

    @Override
    protected void emitTruncateRecord(Receiver receiver, TableSchema tableSchema) throws InterruptedException {
        Struct envelope = tableSchema.getEnvelopeSchema().truncate(getOffset().getSourceInfo(), getClock().currentTimeAsInstant());
        receiver.changeRecord(getPartition(), tableSchema, Operation.TRUNCATE, null, envelope, getOffset(), null);
    }

    @Override
    protected Object[] getOldColumnValues() {

        try {
            switch (getOperation()) {
                case CREATE:
                    return null;
                case UPDATE:
                    return null;
                // return columnValues(message.getOldTupleList(), tableId, true,
                // message.hasTypeMetadata(), true, true);
                default: // vaibhav: I guess the default case is triggered in case of DELETE ops
                    return columnValues(message.getOldTupleList(), tableId, true,
                            message.hasTypeMetadata(), false, true);
            }
        }
        catch (SQLException e) {
            throw new ConnectException(e);
        }
    }

    @Override
    protected Object[] getNewColumnValues() {
        try {
            switch (getOperation()) {
                case CREATE:
                    return columnValues(message.getNewTupleList(), tableId, true, message.hasTypeMetadata(), false, false);
                case UPDATE:
                    // todo vaibhav: add scenario for the case of multiple columns being updated
                    return columnValues(message.getNewTupleList(), tableId, true, message.hasTypeMetadata(), false, false);
                default:
                    return null;
            }
        }
        catch (SQLException e) {
            throw new ConnectException(e);
        }
    }

    private DataCollectionSchema synchronizeTableSchema(DataCollectionSchema tableSchema) {
        if (getOperation() == Operation.DELETE || !message.shouldSchemaBeSynchronized()) {
            return tableSchema;
        }
        final boolean metadataInMessage = message.hasTypeMetadata();
        final TableId tableId = (TableId) tableSchema.id();
        final Table table = schema.tableFor(tableId);
        final List<ReplicationMessage.Column> columns = message.getNewTupleList();
        // CDCSDK we don't need to, as we will get DDL as part of change stream
        // keep updating that.
        // check if we need to refresh our local schema due to DB schema changes for this table
        // if (schemaChanged(columns, table, metadataInMessage)) {
        // // Refresh the schema so we get information about primary keys
        // refreshTableFromDatabase(tableId);
        // // Update the schema with metadata coming from decoder message
        // if (metadataInMessage) {
        // schema.refresh(tableFromFromMessage(columns, schema.tableFor(tableId)));
        // }
        // }
        return schema.schemaFor(tableId);
    }

    private Object[] columnValues(List<ReplicationMessage.Column> columns, TableId tableId,
                                  boolean refreshSchemaIfChanged, boolean metadataInMessage,
                                  boolean sourceOfToasted, boolean oldValues)
            throws SQLException {
        if (columns == null || columns.isEmpty()) {
            return null;
        }
        final Table table = schema.tableFor(tableId);
        if (table == null) {
            schema.dumpTableId();
        }
        Objects.requireNonNull(table);

        // based on the schema columns, create the values on the same position as the columns
        List<Column> schemaColumns = table.columns();
        // based on the replication message without toasted columns for now
        List<ReplicationMessage.Column> columnsWithoutToasted = columns.stream().filter(Predicates.not(ReplicationMessage.Column::isToastedColumn))
                .collect(Collectors.toList());
        // JSON does not deliver a list of all columns for REPLICA IDENTITY DEFAULT
        Object[] values = new Object[columnsWithoutToasted.size() < schemaColumns.size()
                ? schemaColumns.size()
                : columnsWithoutToasted.size()];

        final Set<String> undeliveredToastableColumns = new HashSet<>(schema
                .getToastableColumnsForTableId(table.id()));
        for (ReplicationMessage.Column column : columns) {
            // DBZ-298 Quoted column names will be sent like that in messages,
            // but stored unquoted in the column names
            final String columnName = Strings.unquoteIdentifierPart(column.getName());
            undeliveredToastableColumns.remove(columnName);

            int position = getPosition(columnName, table, values);
            if (position != -1) {
                Object value = column.getValue(() -> (BaseConnection) connection.connection(),
                        connectorConfig.includeUnknownDatatypes());
                values[position] = value;
            }
        }
        return values;
    }

    private int getPosition(String columnName, Table table, Object[] values) {
        final Column tableColumn = table.columnWithName(columnName);

        if (tableColumn == null) {
            logger.warn(
                    "Internal schema is out-of-sync with incoming decoder events; column {} will be omitted from the change event.",
                    columnName);
            return -1;
        }
        int position = tableColumn.position() - 1;
        if (position < 0 || position >= values.length) {
            logger.warn(
                    "Internal schema is out-of-sync with incoming decoder events; column {} will be omitted from the change event.",
                    columnName);
            return -1;
        }
        return position;
    }

    private Optional<DataCollectionSchema> newTable(TableId tableId) {
        logger.info("Schema for table '{}' is missing", tableId);
        refreshTableFromDatabase(tableId);
        final TableSchema tableSchema = schema.schemaFor(tableId);
        logger.info("VKVK table schema now is: " + tableSchema.id());
        if (tableSchema == null) {
            logger.warn("cannot load schema for table '{}'", tableId);
            return Optional.empty();
        }
        else {
            logger.debug("refreshed DB schema to include table '{}'", tableId);
            return Optional.of(tableSchema);
        }
    }

    private void refreshTableFromDatabase(TableId tableId) {
        try {
//            schema.refresh(connection, tableId, connectorConfig.skipRefreshSchemaOnMissingToastableData());
            schema.refresh(connection, tableId, connectorConfig.skipRefreshSchemaOnMissingToastableData(), schema.getSchemaPB());
        }
        catch (SQLException e) {
            throw new ConnectException("Database error while refresing table schema", e);
        }
    }

    static Optional<DataCollectionSchema> updateSchema(TableId tableId,
                                                       ChangeRecordEmitter changeRecordEmitter) {
        return ((YugabyteDBChangeRecordEmitter) changeRecordEmitter).newTable(tableId);
    }

    private boolean schemaChanged(List<ReplicationMessage.Column> columns, Table table,
                                  boolean metadataInMessage) {
        int tableColumnCount = table.columns().size();
        int replicationColumnCount = columns.size();

        boolean msgHasMissingColumns = tableColumnCount > replicationColumnCount;

        if (msgHasMissingColumns && connectorConfig.skipRefreshSchemaOnMissingToastableData()) {
            // if we are ignoring missing toastable data for the purpose of schema sync, we need to modify the
            // hasMissingColumns boolean to account for this. If there are untoasted columns missing from the replication
            // message, we'll still have missing columns and thus require a schema refresh. However, we can /possibly/
            // avoid the refresh if there are only toastable columns missing from the message.
            msgHasMissingColumns = hasMissingUntoastedColumns(table, columns);
        }

        boolean msgHasAdditionalColumns = tableColumnCount < replicationColumnCount;

        if (msgHasMissingColumns || msgHasAdditionalColumns) {
            // the table metadata has less or more columns than the event, which means the table structure has changed,
            // so we need to trigger a refresh...
            logger.info("Different column count {} present in the server message as schema in memory contains {}; refreshing table schema",
                    replicationColumnCount,
                    tableColumnCount);
            return true;
        }

        // go through the list of columns from the message to figure out if any of them are new or have changed their type based
        // on what we have in the table metadata....
        return columns.stream().anyMatch(message -> {
            String columnName = message.getName();
            Column column = table.columnWithName(columnName);
            if (column == null) {
                logger.info("found new column '{}' present in the server message which is not part of the table metadata; refreshing table schema", columnName);
                return true;
            }
            else {
                final int localType = column.nativeType();
                final int incomingType = message.getType().getOid();
                if (localType != incomingType) {
                    final int incomingRootType = message.getType().getRootType().getOid();
                    if (localType != incomingRootType) {
                        logger.info("detected new type for column '{}', old type was {} ({}), new type is {} ({}); refreshing table schema", columnName, localType,
                                column.typeName(),
                                incomingType, message.getType().getName());
                        return true;
                    }
                }
                if (metadataInMessage) {
                    final int localLength = column.length();
                    final int incomingLength = message.getTypeMetadata().getLength();
                    if (localLength != incomingLength) {
                        logger.info("detected new length for column '{}', old length was {}, new length is {}; refreshing table schema", columnName, localLength,
                                incomingLength);
                        return true;
                    }
                    final int localScale = column.scale().orElseGet(() -> 0);
                    final int incomingScale = message.getTypeMetadata().getScale();
                    if (localScale != incomingScale) {
                        logger.info("detected new scale for column '{}', old scale was {}, new scale is {}; refreshing table schema", columnName, localScale,
                                incomingScale);
                        return true;
                    }
                    final boolean localOptional = column.isOptional();
                    final boolean incomingOptional = message.isOptional();
                    if (localOptional != incomingOptional) {
                        logger.info("detected new optional status for column '{}', old value was {}, new value is {}; refreshing table schema", columnName, localOptional,
                                incomingOptional);
                        return true;
                    }
                }
            }
            return false;
        });
    }

    private boolean hasMissingUntoastedColumns(Table table, List<ReplicationMessage.Column> columns) {
        List<String> msgColumnNames = columns.stream()
                .map(ReplicationMessage.Column::getName)
                .collect(Collectors.toList());

        // Compute list of table columns not present in the replication message
        List<String> missingColumnNames = table.columns()
                .stream()
                .filter(c -> !msgColumnNames.contains(c.name()))
                .map(Column::name)
                .collect(Collectors.toList());

        List<String> toastableColumns = schema.getToastableColumnsForTableId(table.id());

        if (logger.isDebugEnabled()) {
            logger.debug("msg columns: '{}' --- missing columns: '{}' --- toastableColumns: '{}",
                    String.join(",", msgColumnNames),
                    String.join(",", missingColumnNames),
                    String.join(",", toastableColumns));
        }
        // Return `true` if we have some columns not in the replication message that are not toastable or that we do
        // not recognize
        return !toastableColumns.containsAll(missingColumnNames);
    }

    private Table tableFromFromMessage(List<ReplicationMessage.Column> columns, Table table) {
        final TableEditor combinedTable = table.edit()
                .setColumns(columns.stream()
                        .map(column -> {
                            final YugabyteDBType type = column.getType();
                            final ColumnEditor columnEditor = Column.editor()
                                    .name(column.getName())
                                    .jdbcType(type.getRootType().getJdbcId())
                                    .type(type.getName())
                                    .optional(column.isOptional())
                                    .nativeType(type.getRootType().getOid());
                            columnEditor.length(column.getTypeMetadata().getLength());
                            columnEditor.scale(column.getTypeMetadata().getScale());

                            // as long as default value is not added to the decoded message metadata, we must apply
                            // the current default read from the database
                            Optional.ofNullable(table.columnWithName(column.getName()))
                                    .map(Column::defaultValue)
                                    .ifPresent(columnEditor::defaultValue);

                            return columnEditor.create();
                        })
                        .collect(Collectors.toList()));
        final List<String> pkCandidates = new ArrayList<>(table.primaryKeyColumnNames());
        final Iterator<String> itPkCandidates = pkCandidates.iterator();
        while (itPkCandidates.hasNext()) {
            final String candidateName = itPkCandidates.next();
            if (!combinedTable.hasUniqueValues() && combinedTable.columnWithName(candidateName) == null) {
                logger.error("Potentional inconsistency in key for message {}", columns);
                itPkCandidates.remove();
            }
        }
        combinedTable.setPrimaryKeyNames(pkCandidates);
        return combinedTable.create();
    }

    @Override
    protected boolean skipEmptyMessages() {
        return true;
    }

    // In case of YB, the update schema is different
    @Override
    protected void emitUpdateRecord(Receiver receiver, TableSchema tableSchema)
            throws InterruptedException {

        Object[] oldColumnValues = getOldColumnValues();
        Object[] newColumnValues = getNewColumnValues();

        Struct oldKey = tableSchema.keyFromColumnData(oldColumnValues);
        Struct newKey = tableSchema.keyFromColumnData(newColumnValues);

        Struct newValue = tableSchema.valueFromColumnData(newColumnValues);
        Struct oldValue = tableSchema.valueFromColumnData(oldColumnValues);

        if (skipEmptyMessages() && (newColumnValues == null || newColumnValues.length == 0)) {
            logger.warn("no new values found for table '{}' from update message at '{}'; skipping record", tableSchema, getOffset().getSourceInfo());
            return;
        }
        // some configurations does not provide old values in case of updates
        // in this case we handle all updates as regular ones
        if (oldKey == null || Objects.equals(oldKey, newKey)) {
            Struct envelope = tableSchema.getEnvelopeSchema().update(oldValue, newValue, getOffset().getSourceInfo(), getClock().currentTimeAsInstant());
            receiver.changeRecord(getPartition(), tableSchema, Operation.UPDATE, newKey, envelope, getOffset(), null);
        }
        // PK update -> emit as delete and re-insert with new key
        else {
            ConnectHeaders headers = new ConnectHeaders();
            headers.add(PK_UPDATE_NEWKEY_FIELD, newKey, tableSchema.keySchema());

            Struct envelope = tableSchema.getEnvelopeSchema().delete(oldValue, getOffset().getSourceInfo(), getClock().currentTimeAsInstant());
            receiver.changeRecord(getPartition(), tableSchema, Operation.DELETE, oldKey, envelope, getOffset(), headers);

            headers = new ConnectHeaders();
            headers.add(PK_UPDATE_OLDKEY_FIELD, oldKey, tableSchema.keySchema());

            envelope = tableSchema.getEnvelopeSchema().create(newValue, getOffset().getSourceInfo(), getClock().currentTimeAsInstant());
            receiver.changeRecord(getPartition(), tableSchema, Operation.CREATE, newKey, envelope, getOffset(), headers);
        }
    }

    @Override
    protected void emitDeleteRecord(Receiver receiver, TableSchema tableSchema) throws InterruptedException {
        Object[] oldColumnValues = getOldColumnValues();
        Struct oldKey = tableSchema.keyFromColumnData(oldColumnValues);
        Struct oldValue = tableSchema.valueFromColumnData(oldColumnValues);

        if (skipEmptyMessages() && (oldColumnValues == null || oldColumnValues.length == 0)) {
            logger.warn("no old values found for table '{}' from delete message at '{}'; skipping record", tableSchema, getOffset().getSourceInfo());
            return;
        }

        Struct envelope = tableSchema.getEnvelopeSchema().delete(oldValue, getOffset().getSourceInfo(), getClock().currentTimeAsInstant());
        receiver.changeRecord(getPartition(), tableSchema, Operation.DELETE, oldKey, envelope, getOffset(), null);
    }
}
