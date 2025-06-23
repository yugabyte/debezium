package io.debezium.connector.postgresql.cdcstate;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;;

/**
 * CDCStateRow represents a row in the CDC state table.
 * It contains information about the tablet ID, stream ID, operation ID, data, and last replication time.
 */
public class CDCStateRow {
    private String tabletId;
    private String streamId;
    private OpId opId;
    private JsonNode data;
    private Timestamp lastReplicationTime;

    /**
     * Constructor for CDCStateRow.
     *
     * @param tabletId            The ID of the tablet.
     * @param streamId            The ID of the stream.
     * @param opId                The operation ID.
     * @param data                The JSON string representation of the CDC state.
     * @param lastReplicationTime The last replication time as a string.
     */
    public CDCStateRow(String tabletId, String streamId, OpId opId, Map<String, String> data, Instant lastReplicationTime) {
        this.tabletId = tabletId;
        this.streamId = streamId;
        this.opId = opId;
        this.data = parseData(data);
        this.lastReplicationTime = lastReplicationTime == null ? null : Timestamp.from(lastReplicationTime);
    }

    public String getTabletId() {
        return tabletId;
    }

    public String getStreamId() {
        return streamId;
    }

    public OpId getOpId() {
        return opId;
    }

    public JsonNode getData() {
        return data;
    }

    public Timestamp getLastReplicationTime() {
        return lastReplicationTime;
    }

    @Override
    public String toString() {
        return "CDCStateRow{" +
                "tabletId='" + tabletId + '\'' +
                ", streamId='" + streamId + '\'' +
                ", opId=" + opId +
                ", data=" + data +
                ", lastReplicationTime=" + lastReplicationTime +
                '}';
    }

    private JsonNode parseData(Map<String, String> data) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.valueToTree(data);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse JSON data", e);
        }
    }

    public static Timestamp parseLastReplicationTime(String tsString) {
        // Cases when the cdc_state table entry hasn't been updated even once.
        if (tsString == null || tsString.isEmpty()) {
            return null;
        }

        // This is the pattern we get the lastReplicationTime:
        //  yyyy-MM-dd    date
        //  HH:mm:ss      time
        //  .SSSSSS       six-digit fraction
        //  XX            zone offset without colon (“+0000”)
        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSXX");
        
        // parse into an OffsetDateTime
        OffsetDateTime odt = OffsetDateTime.parse(tsString, fmt);
        
        // convert to java.sql.Timestamp (in UTC)
        return Timestamp.from(odt.toInstant());
    }
}
