/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresSchema;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;

/**
 * Unit tests for PostgresReplicationConnection publication table comparison logic.
 * Adapted from upstream DBZ-9395 for the YugabyteDB Debezium 2.5.2 fork.
 */
public class PostgresPublicationTableComparisonTest {

    @Mock
    private PostgresConnectorConfig connectorConfig;

    @Mock
    private PostgresConnection jdbcConnection;

    @Mock
    private RelationalTableFilters tableFilter;

    @Mock
    private TypeRegistry typeRegistry;

    @Mock
    private PostgresSchema schema;

    @Mock
    private Statement statement;

    @Mock
    private Connection connection;

    @Mock
    private PreparedStatement preparedStatement;

    @Mock
    private ResultSet resultSet;

    private PostgresReplicationConnection replicationConnection;
    private static final String TEST_PUBLICATION_NAME = "test_publication";
    private static final String TEST_DATABASE_NAME = "test_db";

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        when(connectorConfig.databaseName()).thenReturn(TEST_DATABASE_NAME);
        when(connectorConfig.getJdbcConfig()).thenReturn(JdbcConfiguration.create().build());

        replicationConnection = createTestReplicationConnection();

        when(statement.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
    }

    private PostgresReplicationConnection createTestReplicationConnection() throws Exception {
        return (PostgresReplicationConnection) ReplicationConnection.builder(connectorConfig)
                .withSlot("test_slot")
                .withPublication(TEST_PUBLICATION_NAME)
                .withTableFilter(tableFilter)
                .withPublicationAutocreateMode(PostgresConnectorConfig.AutoCreateMode.FILTERED)
                .withPlugin(PostgresConnectorConfig.LogicalDecoder.PGOUTPUT)
                .dropSlotOnClose(false)
                .statusUpdateInterval(Duration.ofSeconds(10))
                .jdbcMetadataConnection(jdbcConnection)
                .withTypeRegistry(typeRegistry)
                .streamParams("")
                .withSchema(schema)
                .build();
    }

    @Test
    public void testPublicationUpdateRequiredWhenTablesAdded() throws Exception {
        Set<TableId> currentTables = new HashSet<>(Arrays.asList(
                new TableId(null, "public", "customers"),
                new TableId(null, "inventory", "orders")));
        mockGetCurrentPublicationTables(currentTables);

        Set<TableId> capturedTables = new HashSet<>(Arrays.asList(
                new TableId(null, "public", "customers"),
                new TableId(null, "inventory", "orders"),
                new TableId(null, "public", "products")));
        mockDetermineCapturedTables(capturedTables);

        boolean result = replicationConnection.isPublicationUpdateRequired(statement);

        assertThat(result).isTrue();
        verify(connection).prepareStatement(eq(String.format(
                "SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = '%s'",
                TEST_PUBLICATION_NAME)));
        verify(preparedStatement).executeQuery();
    }

    @Test
    public void testPublicationUpdateRequiredOnSQLException() throws Exception {
        when(preparedStatement.executeQuery()).thenThrow(
                new SQLException("permission denied for relation pg_publication_tables"));

        Set<TableId> capturedTables = new HashSet<>(Arrays.asList(
                new TableId(null, "public", "customers")));
        mockDetermineCapturedTables(capturedTables);

        boolean result = replicationConnection.isPublicationUpdateRequired(statement);

        assertThat(result).isTrue();
    }

    @Test
    public void testPublicationUpdateRequiredWhenPublicationEmpty() throws Exception {
        when(resultSet.next()).thenReturn(false);

        Set<TableId> capturedTables = new HashSet<>(Arrays.asList(
                new TableId(null, "public", "customers")));
        mockDetermineCapturedTables(capturedTables);

        boolean result = replicationConnection.isPublicationUpdateRequired(statement);

        assertThat(result).isTrue();
    }

    @Test
    public void testNoUpdateRequiredWhenTablesMatch() throws Exception {
        Set<TableId> tables = new HashSet<>(Arrays.asList(
                new TableId(null, "public", "customers"),
                new TableId(null, "inventory", "orders")));

        mockGetCurrentPublicationTables(tables);
        mockDetermineCapturedTables(tables);

        boolean result = replicationConnection.isPublicationUpdateRequired(statement);

        assertThat(result).isFalse();
    }

    @Test
    public void testPublicationUpdateRequiredWhenTablesDiffer() throws Exception {
        Set<TableId> currentTables = new HashSet<>(Arrays.asList(
                new TableId(null, "public", "customers")));

        Set<TableId> desiredTables = new HashSet<>(Arrays.asList(
                new TableId(null, "public", "customers"),
                new TableId(null, "inventory", "orders")));

        mockGetCurrentPublicationTables(currentTables);
        mockDetermineCapturedTables(desiredTables);

        boolean result = replicationConnection.isPublicationUpdateRequired(statement);

        assertThat(result).isTrue();
    }

    @Test
    public void testPublicationUpdateRequiredWhenQueryFails() throws Exception {
        mockGetCurrentPublicationTables(null);

        Set<TableId> desiredTables = new HashSet<>(Arrays.asList(
                new TableId(null, "public", "customers")));
        mockDetermineCapturedTables(desiredTables);

        boolean result = replicationConnection.isPublicationUpdateRequired(statement);

        assertThat(result).isTrue();
    }

    @Test
    public void testNoUpdateRequiredWhenNoDesiredTables() throws Exception {
        Set<TableId> currentTables = new HashSet<>(Arrays.asList(
                new TableId(null, "public", "customers")));

        mockGetCurrentPublicationTables(currentTables);
        mockDetermineCapturedTables(new HashSet<>());

        boolean result = replicationConnection.isPublicationUpdateRequired(statement);

        assertThat(result).isFalse();
    }

    private void mockGetCurrentPublicationTables(Set<TableId> tables) throws Exception {
        if (tables == null) {
            when(preparedStatement.executeQuery()).thenThrow(new SQLException("permission denied"));
        }
        else if (tables.isEmpty()) {
            when(resultSet.next()).thenReturn(false);
        }
        else {
            Boolean[] nextResults = new Boolean[tables.size() + 1];
            String[] schemaNames = new String[tables.size()];
            String[] tableNames = new String[tables.size()];

            int i = 0;
            for (TableId tableId : tables) {
                nextResults[i] = true;
                schemaNames[i] = tableId.schema();
                tableNames[i] = tableId.table();
                when(jdbcConnection.createTableId(TEST_DATABASE_NAME, tableId.schema(), tableId.table()))
                        .thenReturn(tableId);
                i++;
            }
            nextResults[i] = false;

            when(resultSet.next()).thenReturn(nextResults[0], Arrays.copyOfRange(nextResults, 1, nextResults.length));
            when(resultSet.getString("schemaname")).thenReturn(schemaNames[0], Arrays.copyOfRange(schemaNames, 1, schemaNames.length));
            when(resultSet.getString("tablename")).thenReturn(tableNames[0], Arrays.copyOfRange(tableNames, 1, tableNames.length));
        }
    }

    private void mockDetermineCapturedTables(Set<TableId> tables) throws Exception {
        when(jdbcConnection.getAllTableIds(TEST_DATABASE_NAME)).thenReturn(tables);
        when(tableFilter.dataCollectionFilter()).thenReturn(tableId -> tables.contains(tableId));
    }
}
