import org.yb.client.AsyncYBClient;
import org.yb.client.ListTablesResponse;
import org.yb.client.YBClient;
import org.yb.client.YBTable;
import org.yb.master.MasterDdlOuterClass.ListTablesResponsePB.TableInfo;
import java.util.Set;   
import java.util.HashSet;

import java.util.Objects;


public class Main {
    public static void main(String[] args) throws Exception {
        CmdLineParams parameters = CmdLineParams.createFromArgs(args);
        AsyncYBClient asyncClient = new AsyncYBClient.AsyncYBClientBuilder(parameters.masterAddresses)
                .sslCertFile(parameters.certFilePath) // TODO: support sslClientCertFiles(clientCert, clientKey)
                .build();

        YBClient client = new YBClient(asyncClient);
        YBTable table = null;
        try {
            ListTablesResponse resp = client.getTablesList();
            for (TableInfo tableInfo : resp.getTableInfoList()) {
                if (Objects.equals(tableInfo.getName(), parameters.tableName)) {
                    table =  client.openTableByUUID(tableInfo.getId().toStringUtf8());
                }
            }
        } catch (Exception ex) {
            throw ex;
        }
        if (table == null) {
            throw new RuntimeException("No table found with the specified name");
        }
        if (parameters.toCreate) {
            String streamID = client.createCDCStream(table, parameters.dbName, "PROTO", "IMPLICIT", "ALL").getStreamId();
            System.out.println("CDC Stream ID: " + streamID);
        } else if (parameters.deleteStreamId != null) {
            Set<String> streamIds = new HashSet<String>();
            streamIds.add(parameters.deleteStreamId);
            client.deleteCDCStream(streamIds, false, true);
        } else {
            throw new RuntimeException("Either create or delete stream id should be specified");
        }
    }
}
