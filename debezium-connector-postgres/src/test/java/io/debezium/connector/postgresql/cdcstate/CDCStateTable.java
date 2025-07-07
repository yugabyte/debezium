package io.debezium.connector.postgresql.cdcstate;

import java.net.InetSocketAddress;
import java.sql.Timestamp;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;

/**
 * CDCStateWrapper is a wrapper class for managing the CDC state in a YCQL database.
 * It provides methods to create and retrieve the CDC state rows.
 * 
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class CDCStateTable {
    private static final Logger LOGGER = LoggerFactory.getLogger(CDCStateTable.class);
    private static final String GET_CDC_STATE_STMT = "SELECT * FROM system.cdc_state";
    
    private List<CDCStateRow> cdcStateRows;
    private Timestamp lastRefreshTime;

    public CDCStateTable(List<CDCStateRow> cdcStateRows) {
        this.cdcStateRows = cdcStateRows;
        
        // Initial creation will not indicate a refresh.
        this.lastRefreshTime = null;
    }

    public boolean isEmpty() {
        return cdcStateRows == null || cdcStateRows.isEmpty();
    }

    public void refresh() {
        LOGGER.info("Refreshing CDC state from service...");
        // Refresh the CDC state rows by fetching them again from the database.
        this.cdcStateRows = fetchCdcStateRows();
        
        // Update the last refresh time to the current time.
        this.lastRefreshTime = new Timestamp(System.currentTimeMillis());
    }

    public List<CDCStateRow> getCdcStateRows() {
        return cdcStateRows;
    }

    public static CDCStateTable createCdcState() {
        return new CDCStateTable(fetchCdcStateRows());
    }

    protected static List<CDCStateRow> fetchCdcStateRows() {
        return fetchCdcStateRows("127.0.0.1", 9042, "cassandra", "cassandra", "yugabyte");
    }

    protected static List<CDCStateRow> fetchCdcStateRows(
            String hostName, int port,
            String userName, String password, String keyspace) {
        try {
            CqlSession session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(hostName, port))
                .withKeyspace(keyspace)
                .withAuthCredentials(userName, password)
                .withLocalDatacenter("datacenter1")
                .build();
            
            ResultSet resultSet = session.execute(GET_CDC_STATE_STMT);
            List<CDCStateRow> cdcStateRows = resultSet.all().stream()
                .map(row -> new CDCStateRow(
                    row.getString("tablet_id"),
                    row.getString("stream_id"),
                    OpId.fromString(row.getString("checkpoint")),
                    row.getMap("data", String.class, String.class),
                    row.getInstant("last_replication_time")
                )).collect(Collectors.toList());
            
            return cdcStateRows;
        } catch (Exception e) {
            // Handle exceptions, such as connection errors or query execution errors
            LOGGER.error("Error while fetching CDC state rows: " + e.getMessage());
            
            throw new RuntimeException("Failed to fetch CDC state rows", e);
        }
    }
}
