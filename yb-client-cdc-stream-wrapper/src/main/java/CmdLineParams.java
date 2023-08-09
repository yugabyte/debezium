import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;

public class CmdLineParams {
    String masterAddresses = "127.0.0.1:7100";
    String tableName = "";
    String dbName = "yugabyte";

    String certFilePath = null;

    Boolean toCreate = false;
    String deleteStreamId = null;

    public static CmdLineParams createFromArgs(String[] args) {
        Options options = new Options();

        options.addOption("create", false, "To create CDC stream id");
        options.addOption("delete_stream", true, "To delete CDC stream id");
        options.addOption("master_addresses", true, "Addresses of the master process");
        options.addOption("table_name", true, "Any table name in the database");
        options.addOption("db_name", true, "Database for which stream needs to be created");
        options.addOption("ssl_cert_file", true, "path to certificate file in case of SSL enabled");

        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = null;
        try {
            commandLine = parser.parse(options, args);
        } catch (Exception e) {
            System.out.println("Exception while parsing arguments: " + e);
            System.exit(-1);
        }

        CmdLineParams params = new CmdLineParams();
        params.initialize(commandLine);
        return params;
    }

    private void initialize(CommandLine commandLine) {
        if (commandLine.hasOption("master_addresses")) {
            masterAddresses = commandLine.getOptionValue("master_addresses");
        }

        if (commandLine.hasOption("table_name")) {
            tableName = commandLine.getOptionValue("table_name");
        }

        if (commandLine.hasOption("db_name")) {
            dbName = commandLine.getOptionValue("db_name");
        }

        if (commandLine.hasOption("ssl_cert_file")) {
            certFilePath = commandLine.getOptionValue("ssl_cert_file");
        }

        if (commandLine.hasOption("create")) {
            toCreate = true;
        }

        if (commandLine.hasOption("delete_stream")) {
            deleteStreamId = commandLine.getOptionValue("delete_stream");
        }
    }

}
