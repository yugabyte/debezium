package io.debezium.connector.postgresql;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.List;

/**
 * Tests to verify that our validation methods are working fine.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YBValidateTest {
    @Test
    public void shouldThrowExceptionWhenSlotsAndPublicationsDoNotMatch() {
        List<String> slots = List.of("a", "b");
        List<String> publications = List.of("pub");

        try {
            YBValidate.slotAndPublicationsAreEqual(slots, publications);
        } catch (DebeziumException ex) {
            assertTrue(ex.getMessage().contains("Number of provided slots does not match the number of provided publications"));
        }
    }

    @Test
    public void shouldThrowExceptionWhenSlotsAndSlotRangesDoNotMatch() {
        List<String> slots = List.of("a", "b", "c");
        List<String> slotRanges = List.of("0,10", "10,1000");

        try {
            YBValidate.slotRangesMatchSlotNames(slots, slotRanges);
        } catch (DebeziumException ex) {
            assertTrue(ex.getMessage().contains("Number of provided slots does not match the number of provided slot ranges"));
        }
    }

    @Test
    public void shouldThrowExceptionWhenEndBoundaryIsMissing() {
        List<String> slotRanges = List.of("0,10", "10,1000");

        try {
            YBValidate.completeRangesProvided(slotRanges);
        } catch (DebeziumException ex) {
            assertTrue(ex.getMessage().contains("Incomplete ranges provided"));
        }
    }

    @Test
    public void shouldThrowExceptionWhenMidRangeIsMissing() {
        List<String> slotRanges = List.of("0,6553", "13107,19660", "19660,26214", "26214,32768", "32768,39321", "39321,45875", "45875,52428", "52428,58982", "58982,65536");

        try {
            YBValidate.completeRangesProvided(slotRanges);
        } catch (DebeziumException ex) {
            assertTrue(ex.getMessage().contains("Tablet range starting from hash_code"));
        }
    }

    @Test
    public void shouldRejectUnsupportedSlotSeekToKnownOffsetProperty() {
        Configuration config = Configuration.create()
                .with(PostgresConnectorConfig.SLOT_SEEK_TO_KNOWN_OFFSET, true)
                .build();

        DebeziumException ex = assertThrows(DebeziumException.class,
                () -> YugabyteDBConnector.rejectUnsupportedProperties(config));

        assertTrue(ex.getMessage().contains(PostgresConnectorConfig.SLOT_SEEK_TO_KNOWN_OFFSET.name()));
        assertTrue(ex.getMessage().contains("no longer supported"));
    }

    @Test
    public void shouldRejectUnsupportedSlotSeekToKnownOffsetPropertyEvenWhenFalse() {
        Configuration config = Configuration.create()
                .with(PostgresConnectorConfig.SLOT_SEEK_TO_KNOWN_OFFSET, false)
                .build();

        assertThrows(DebeziumException.class,
                () -> YugabyteDBConnector.rejectUnsupportedProperties(config));
    }
}
