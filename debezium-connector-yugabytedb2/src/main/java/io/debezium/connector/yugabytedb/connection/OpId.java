package io.debezium.connector.yugabytedb.connection;

import java.util.Arrays;
import java.util.Base64;

import com.google.common.base.Objects;

public class OpId implements Comparable<OpId> {
    public long getTerm() {
        return term;
    }

    public long getIndex() {
        return index;
    }

    public byte[] getKey() {
        return key;
    }

    public int getWrite_id() {
        return write_id;
    }

    private long term;
    private long index;
    private byte[] key;
    private int write_id;

    public OpId(long term, long index, byte[] key, int write_id) {
        this.term = term;
        this.index = index;
        this.key = key;
        this.write_id = write_id;
    }

    public static OpId valueOf(String stringId) {
        if (stringId != null && !stringId.isEmpty()) {
            String[] arr = stringId.split(":");
            return new OpId(Long.valueOf(arr[0]),
                    Long.valueOf(arr[1]),
                    Base64.getDecoder().decode(arr[2]),
                    Integer.valueOf(arr[3]));
        }
        return null;
    }

    public String toSerString() {
        String keyStr = Base64.getEncoder().encodeToString(key);

        return term + ":" + index + ":" + keyStr + ":" + write_id;
    }

    @Override
    public String toString() {
        return "" +
                "term=" + term +
                ", index=" + index +
                ", key=" + Arrays.toString(key) +
                ", write_id=" + write_id +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        OpId that = (OpId) o;
        return term == that.term && index == that.index && write_id == that.write_id && Objects.equal(key, that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(term, index, key, write_id);
    }

    @Override
    public int compareTo(OpId o) {
        // Unsigned comparison
        if (term != o.term)
            return term + Long.MIN_VALUE < o.term + Long.MIN_VALUE ? -1 : 1;
        else if (index != o.index)
            return index + Long.MIN_VALUE < o.index + Long.MIN_VALUE ? -1 : 1;
        else
            return write_id + Long.MIN_VALUE < o.write_id + Long.MIN_VALUE ? -1 : 1;
    }
}
