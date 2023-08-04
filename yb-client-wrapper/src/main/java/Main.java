import org.yb.client.AsyncYBClient;
import org.yb.client.ListTablesResponse;
import org.yb.client.YBClient;
import org.yb.client.YBTable;
import org.yb.master.MasterDdlOuterClass.ListTablesResponsePB.TableInfo;

import java.util.Objects;


public class Main {
    public static void main(String[] args) throws Exception {
        CmdLineParams parameters = CmdLineParams.createFromArgs(args);
        AsyncYBClient asyncClient = new AsyncYBClient.AsyncYBClientBuilder(parameters.masterAddresses)
                .sslCertFile(parameters.certFilePath)
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
            throw new NullPointerException("No table found with the specified name");
        }
        String streamID = client.createCDCStream(table, parameters.dbName, "PROTO", "IMPLICIT", "ALL").getStreamId();
        System.out.println("CDC Stream ID: " + streamID);
    }
}
