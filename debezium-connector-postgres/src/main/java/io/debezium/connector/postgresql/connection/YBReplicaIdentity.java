package io.debezium.connector.postgresql.connection;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Helper class to assist in storing the replica identity information of a given table.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YBReplicaIdentity {
  private static final Logger LOGGER = LoggerFactory.getLogger(YBReplicaIdentity.class);

  private final String GET_REPLICA_IDENTITY_FORMAT = "SELECT relreplident FROM pg_class WHERE oid = '%s'::regclass;";

  private final PostgresConnectorConfig connectorConfig;
  private final TableId tableId;
  private ReplicaIdentityInfo.ReplicaIdentity replicaIdentity;

  public YBReplicaIdentity(PostgresConnectorConfig connectorConfig, TableId tableId) {
    this.connectorConfig = connectorConfig;
    this.tableId = tableId;
  }

  /**
   * @return the {@link io.debezium.connector.postgresql.connection.ReplicaIdentityInfo.ReplicaIdentity} for the given table.
   */
  public ReplicaIdentityInfo.ReplicaIdentity getReplicaIdentity() {
    if (replicaIdentity == null) {
      LOGGER.info("No replica identity cached for table {}, fetching from service", tableId.toString());
      try (PostgresConnection pgConn = new PostgresConnection(connectorConfig.getJdbcConfig(), PostgresConnection.CONNECTION_FETCH_REPLICA_IDENTITY)) {
        try (Connection conn = pgConn.connection()) {
          Statement st = conn.createStatement();
          ResultSet rs = st.executeQuery(String.format(GET_REPLICA_IDENTITY_FORMAT, tableId.identifier()));

          if (rs.next()) {
            replicaIdentity = ReplicaIdentityInfo.ReplicaIdentity.parseFromDB(rs.getString("relreplident"));

            return replicaIdentity;
          }
        } catch (SQLException sqle) {
          LOGGER.error("Exception while fetching replica identity for table " + tableId, sqle);
          throw new RuntimeException(sqle);
        }
      }
    }

    return replicaIdentity;
  }
}
