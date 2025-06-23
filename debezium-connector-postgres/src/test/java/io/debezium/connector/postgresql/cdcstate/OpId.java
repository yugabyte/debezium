package io.debezium.connector.postgresql.cdcstate;

public class OpId {
    private final long term;
    private final long index;

    public OpId(long term, long index) {
        this.term = term;
        this.index = index;
    }

    public long getTerm() {
        return term;
    }

    public long getIndex() {
        return index;
    }

    public static OpId fromString(String opId) {
        if (opId == null || opId.isEmpty()) {
            // OpId will be null for the entry where tablet ID value is "dummy_id_for_replication_slot"
            return null;
        }


        String[] parts = opId.split("\\.");
        
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid OpId format: " + opId);
        }

        return new OpId(Long.parseLong(parts[0]), Long.parseLong(parts[1]));
    }
}
