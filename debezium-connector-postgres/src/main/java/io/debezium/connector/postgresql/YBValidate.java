package io.debezium.connector.postgresql;

import io.debezium.DebeziumException;
import io.debezium.connector.postgresql.transforms.yugabytedb.Pair;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Class to store all the validation methods.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YBValidate {
    private static final String RANGE_BEGIN = "0";
    private static final String RANGE_END = "65536";

    public static void completeRangesProvided(List<String> slotRanges) {
        List<Pair<String, String>> pairList = slotRanges.stream()
                .map(entry -> {
                    String[] parts = entry.split(",");
                    return new Pair<>(parts[0], parts[1]);
                })
                .sorted(Comparator.comparing(Pair::getFirst))
                .collect(Collectors.toList());

        String rangeBegin = RANGE_BEGIN;

        for (Pair<String, String> pair : pairList) {
            if (!rangeBegin.equals(pair.getFirst())) {
                throw new DebeziumException(
                    String.format("Tablet range starting from hash_code %s is missing", rangeBegin));
            }

            rangeBegin = pair.getSecond();
        }

        // At this point, if the range is complete, rangeBegin will be pointing to the RANGE_END value.
        if (!rangeBegin.equals(RANGE_END)) {
            throw new DebeziumException(
                String.format("Incomplete ranges provided. Range starting from hash_code %s is missing", rangeBegin));
        }
    }

    public static void slotAndPublicationsAreEqual(List<String> slotNames, List<String> publicationNames) {
        if (slotNames.size() != publicationNames.size()) {
            throw new DebeziumException(
                String.format("Number of provided slots does not match the number of provided " +
                                "publications. Slots: %s, Publications: %s", slotNames, publicationNames));
        }
    }

    public static void slotRangesMatchSlotNames(List<String> slotNames, List<String> slotRanges) {
        if (slotNames.size() != slotRanges.size()) {
            throw new DebeziumException(
                    String.format("Number of provided slots does not match the number of provided " +
                            "slot ranges. Slots: %s, Slot ranges: %s", slotNames, slotRanges));
        }
    }
}
