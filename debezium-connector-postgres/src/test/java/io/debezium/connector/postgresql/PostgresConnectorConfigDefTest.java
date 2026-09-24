/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigValue;
import org.junit.Test;

import io.debezium.config.ConfigDefinitionMetadataTest;
import io.debezium.config.Configuration;

public class PostgresConnectorConfigDefTest extends ConfigDefinitionMetadataTest {

    public PostgresConnectorConfigDefTest() {
        super(new YugabyteDBConnector());
    }

    @Test
    public void shouldSetReplicaAutoSetValidValue() {

        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.REPLICA_IDENTITY_AUTOSET_VALUES, "testSchema_1.testTable_1:FULL,testSchema_2.testTable_2:DEFAULT");

        int problemCount = PostgresConnectorConfig.validateReplicaAutoSetField(
                configBuilder.build(), PostgresConnectorConfig.REPLICA_IDENTITY_AUTOSET_VALUES, (field, value, problemMessage) -> System.out.println(problemMessage));

        assertThat((problemCount == 0)).isTrue();
    }

    @Test
    public void shouldSetReplicaAutoSetInvalidValue() {

        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.REPLICA_IDENTITY_AUTOSET_VALUES, "testSchema_1.testTable_1;FULL,testSchema_2.testTable_2;;DEFAULT");

        int problemCount = PostgresConnectorConfig.validateReplicaAutoSetField(
                configBuilder.build(), PostgresConnectorConfig.REPLICA_IDENTITY_AUTOSET_VALUES, (field, value, problemMessage) -> System.out.println(problemMessage));

        assertThat((problemCount == 2)).isTrue();
    }

    @Test
    public void shouldSetReplicaAutoSetRegExValue() {

        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.REPLICA_IDENTITY_AUTOSET_VALUES, ".*.test.*:FULL,testSchema_2.*:DEFAULT");

        int problemCount = PostgresConnectorConfig.validateReplicaAutoSetField(
                configBuilder.build(), PostgresConnectorConfig.REPLICA_IDENTITY_AUTOSET_VALUES, (field, value, problemMessage) -> System.out.println(problemMessage));

        assertThat((problemCount == 0)).isTrue();
    }

    @Test
    public void shouldValidateWithCorrectSingleHostnamePattern() {
        validateCorrectHostname(false);
    }

    @Test
    public void shouldValidateWithCorrectMultiHostnamePattern() {
        validateCorrectHostname(true);
    }

    @Test
    public void shouldFailWithInvalidCharacterInHostname() {
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.HOSTNAME, "*invalidCharacter");

        int problemCount = PostgresConnectorConfig.validateYBHostname(
          configBuilder.build(), PostgresConnectorConfig.HOSTNAME, (field, value, problemMessage) -> System.out.println(problemMessage));

        assertThat((problemCount == 1)).isTrue();
    }

    @Test
    public void shouldFailIfInvalidMultiHostFormatSpecified() {
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.HOSTNAME, "127.0.0.1,127.0.0.2,127.0.0.3");

        int problemCount = PostgresConnectorConfig.validateYBHostname(
          configBuilder.build(), PostgresConnectorConfig.HOSTNAME, (field, value, problemMessage) -> System.out.println(problemMessage));

        assertThat((problemCount == 1)).isTrue();
    }

    @Test
    public void shouldFailIfInvalidMultiHostFormatSpecifiedWithInvalidCharacter() {
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.HOSTNAME, "127.0.0.1,127.0.0.2,127.0.0.3+");

        int problemCount = PostgresConnectorConfig.validateYBHostname(
          configBuilder.build(), PostgresConnectorConfig.HOSTNAME, (field, value, problemMessage) -> System.out.println(problemMessage));

        assertThat((problemCount == 2)).isTrue();
    }

    @Test
    public void shouldFailIfSlotRangesSpecifiedWithoutParallelStreamingMode() {
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.STREAMING_MODE, PostgresConnectorConfig.StreamingMode.DEFAULT)
                .with(PostgresConnectorConfig.SLOT_RANGES, "0,10;10,65536");

        boolean valid = PostgresConnectorConfig.SLOT_RANGES.validate(
                configBuilder.build(), (field, value, problemMessage) -> System.out.println(problemMessage));

        assertThat(valid).isFalse();
    }

    @Test
    public void ensureNoErrorWhenProperParallelStreamingConfigSpecified() {
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.STREAMING_MODE, PostgresConnectorConfig.StreamingMode.PARALLEL)
                .with(PostgresConnectorConfig.SLOT_RANGES, "0,10;10,65536");

        boolean valid = PostgresConnectorConfig.SLOT_RANGES.validate(
                configBuilder.build(), (field, value, problemMessage) -> System.out.println(problemMessage));

        assertThat(valid).isTrue();
    }

    @Test
    public void shouldFailForInvalidYbLoadBalanceConnectionsValue() {
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.YB_LOAD_BALANCE_CONNECTIONS, "invalid");

        int problemCount = PostgresConnectorConfig.validateYbLoadBalanceConnectionsValue(
                configBuilder.build(), PostgresConnectorConfig.YB_LOAD_BALANCE_CONNECTIONS, (field, value, problemMessage) -> System.out.println(problemMessage));

        assertThat((problemCount == 1)).isTrue();
    }

    @Test
    public void shouldDefaultLsnFlushModeToConnector() {
        Configuration configuration = TestHelper.defaultConfig().build();
        PostgresConnectorConfig config = new PostgresConnectorConfig(configuration);

        assertThat(config.getLsnFlushMode()).isEqualTo(PostgresConnectorConfig.LsnFlushMode.CONNECTOR);
        assertThat(config.isFlushLsnOnSource()).isTrue();

        // a configuration that does not mention lsn.flush.mode at all must pass the full validation
        Map<String, ConfigValue> results = configuration.validate(PostgresConnectorConfig.ALL_FIELDS);
        assertThat(results.get(PostgresConnectorConfig.LSN_FLUSH_MODE.name()).errorMessages()).isEmpty();
        assertThat(results.values().stream().filter(v -> !v.errorMessages().isEmpty()).map(ConfigValue::name)).isEmpty();
    }

    @Test
    public void shouldMapDeprecatedFlushLsnSourceToLsnFlushMode() {
        PostgresConnectorConfig manual = new PostgresConnectorConfig(TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SHOULD_FLUSH_LSN_IN_SOURCE_DB, false)
                .build());
        assertThat(manual.getLsnFlushMode()).isEqualTo(PostgresConnectorConfig.LsnFlushMode.MANUAL);
        assertThat(manual.isFlushLsnOnSource()).isFalse();

        PostgresConnectorConfig connector = new PostgresConnectorConfig(TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SHOULD_FLUSH_LSN_IN_SOURCE_DB, true)
                .build());
        assertThat(connector.getLsnFlushMode()).isEqualTo(PostgresConnectorConfig.LsnFlushMode.CONNECTOR);

        // the new option wins when both are set
        PostgresConnectorConfig both = new PostgresConnectorConfig(TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SHOULD_FLUSH_LSN_IN_SOURCE_DB, false)
                .with(PostgresConnectorConfig.LSN_FLUSH_MODE, "connector_and_driver")
                .build());
        assertThat(both.getLsnFlushMode()).isEqualTo(PostgresConnectorConfig.LsnFlushMode.CONNECTOR_AND_DRIVER);
    }

    @Test
    public void shouldRejectInvalidLsnFlushMode() {
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.LSN_FLUSH_MODE, "sometimes")
                .build();

        List<String> problems = new ArrayList<>();
        boolean valid = PostgresConnectorConfig.LSN_FLUSH_MODE.validate(config, (field, value, problemMessage) -> problems.add(problemMessage));

        assertThat(valid).isFalse();
        assertThat(problems).isNotEmpty();
    }

    @Test
    public void shouldRejectDriverKeepaliveFlushForHybridTimeSlot() {
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SLOT_LSN_TYPE, "HYBRID_TIME")
                .with(PostgresConnectorConfig.LSN_FLUSH_MODE, "connector_and_driver")
                .build();

        List<String> problems = new ArrayList<>();
        boolean valid = PostgresConnectorConfig.LSN_FLUSH_MODE.validate(config, (field, value, problemMessage) -> problems.add(problemMessage));

        assertThat(valid).isFalse();
        assertThat(problems).hasSize(1);
        assertThat(problems.get(0)).contains("not allowed with slot.lsn.type=HYBRID_TIME");

        // the same rule is enforced by the full configuration validation used by the connector and the task
        Map<String, ConfigValue> results = config.validate(PostgresConnectorConfig.ALL_FIELDS);
        assertThat(results.get(PostgresConnectorConfig.LSN_FLUSH_MODE.name()).errorMessages()).hasSize(1);
    }

    @Test
    public void shouldAllowDriverKeepaliveFlushForSequenceSlot() {
        Configuration explicitSequence = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SLOT_LSN_TYPE, "SEQUENCE")
                .with(PostgresConnectorConfig.LSN_FLUSH_MODE, "connector_and_driver")
                .build();
        Configuration defaultLsnType = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.LSN_FLUSH_MODE, "connector_and_driver")
                .build();

        for (Configuration config : List.of(explicitSequence, defaultLsnType)) {
            assertThat(PostgresConnectorConfig.LSN_FLUSH_MODE.validate(config, (field, value, problemMessage) -> System.out.println(problemMessage))).isTrue();
            assertThat(config.validate(PostgresConnectorConfig.ALL_FIELDS).get(PostgresConnectorConfig.LSN_FLUSH_MODE.name()).errorMessages()).isEmpty();
            assertThat(new PostgresConnectorConfig(config).getLsnFlushMode()).isEqualTo(PostgresConnectorConfig.LsnFlushMode.CONNECTOR_AND_DRIVER);
        }

        // connector mode is fine with HYBRID_TIME
        Configuration hybridTimeConnectorMode = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SLOT_LSN_TYPE, "HYBRID_TIME")
                .with(PostgresConnectorConfig.LSN_FLUSH_MODE, "connector")
                .build();
        assertThat(PostgresConnectorConfig.LSN_FLUSH_MODE.validate(hybridTimeConnectorMode, (field, value, problemMessage) -> System.out.println(problemMessage))).isTrue();
    }

    public void validateCorrectHostname(boolean multiNode) {
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.HOSTNAME, multiNode ? "127.0.0.1:5433,127.0.0.2:5433,127.0.0.3:5433" : "127.0.0.1");

        int problemCount = PostgresConnectorConfig.validateYBHostname(
          configBuilder.build(), PostgresConnectorConfig.HOSTNAME, (field, value, problemMessage) -> System.out.println(problemMessage));

        assertThat((problemCount == 0)).isTrue();
    }
}
