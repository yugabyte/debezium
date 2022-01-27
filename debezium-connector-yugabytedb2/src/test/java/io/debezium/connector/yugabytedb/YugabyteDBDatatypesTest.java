package io.debezium.connector.yugabytedb;

import com.google.gson.Gson;
import io.debezium.config.Configuration;
import io.debezium.data.SchemaUtil;
import io.debezium.data.VerifyRecord;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Strings;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.source.SourceRecord;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

import java.net.InetAddress;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Basic unit tests to check the behaviour with YugabyteDB datatypes
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */

public class YugabyteDBDatatypesTest extends AbstractConnectorTest {
	private static final String INSERT_STMT = "INSERT INTO s1.a (aa) VALUES (1);" +
			"INSERT INTO s2.a (aa) VALUES (1);";
	private static final String CREATE_TABLES_STMT = "DROP SCHEMA IF EXISTS s1 CASCADE;" +
			"DROP SCHEMA IF EXISTS s2 CASCADE;" +
			"CREATE SCHEMA s1; " +
			"CREATE SCHEMA s2; " +
			"CREATE TABLE s1.a (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
			"CREATE TABLE s2.a (pk SERIAL, aa integer, bb varchar(20), PRIMARY KEY(pk));";
	private static final String SETUP_TABLES_STMT = CREATE_TABLES_STMT + INSERT_STMT;
	private YugabyteDBConnector connector;

	private CompletableFuture<Void> insertRecords(long numOfRowsToBeInserted) {
		String formatInsertString = "INSERT INTO t1 VALUES (%d, 'Vaibhav', 'Kushwaha', 30);";
		return CompletableFuture.runAsync(() -> {
			for (int i = 0; i < numOfRowsToBeInserted; i++) {
				TestHelper.execute(String.format(formatInsertString, i));
			}
		}).exceptionally(throwable -> {
			throw new RuntimeException(throwable);
		});
	}

	// this is not working as the json String is not coming as expected
	// some extra character is coming up while parsing
	protected void /*Map<String, String>*/ getAfterValue(String jsonString) throws Exception {
		System.out.println("VKVK string: " + jsonString);
		JSONObject jsonObj = (JSONObject) new JSONParser().parse(jsonString);

		System.out.println("VKVK value: " + jsonObj.get("value"));
//		return mp;
	}

	protected Configuration.Builder getConfigBuilder() throws Exception {
		return TestHelper.defaultConfig()
				.with(YugabyteDBConnectorConfig.HOSTNAME, "127.0.0.1")
				.with(YugabyteDBConnectorConfig.PORT, 5433)
				.with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, YugabyteDBConnectorConfig.SnapshotMode.NEVER.getValue())
				.with(YugabyteDBConnectorConfig.DELETE_STREAM_ON_STOP, Boolean.TRUE)
				.with(YugabyteDBConnectorConfig.MASTER_HOSTNAME, "127.0.0.1"/*InetAddress.getLocalHost().getHostAddress()*/)
				.with(YugabyteDBConnectorConfig.MASTER_PORT, "7100")
				.with(YugabyteDBConnectorConfig.TABLE_INCLUDE_LIST, "public.t1"); // including t1 for now only
	}

	private void consumeRecords(long recordsCount) {
		int totalConsumedRecords = 0;
		long start = System.currentTimeMillis();
		List<SourceRecord> records = new ArrayList<>();
		while (totalConsumedRecords < recordsCount) {
			int consumed = super.consumeAvailableRecords(record -> {
				System.out.println("VKVK the record being consumed is " + record);
				records.add(record);
			});
			if (consumed > 0) {
				totalConsumedRecords += consumed;
//				System.out.println("VKVK consumed " + totalConsumedRecords + " records");
			}
		}
		System.out.println("total duration to ingest '" + recordsCount + "' records: " +
				Strings.duration(System.currentTimeMillis() - start));

//		System.out.println("VKVK printing the inserted records now: ");
		for (int i = 0; i < records.size(); ++i) {
			// verify the records
			System.out.println(String.format("VKVK verifying record with pk: <id = %d>", i));
			assertInsert(records.get(i), "id", i);
//			VerifyRecord.isValidInsert(records.get(i), "id", i);
		}
	}

	private void printRecordsJson(long recordsCount) {
		int totalConsumedRecords = 0;
		long start = System.currentTimeMillis();
		List<SourceRecord> records = new ArrayList<>();
		while (totalConsumedRecords < recordsCount) {
			int consumed = super.consumeAvailableRecords(record -> {
				System.out.println("VKVK the record being consumed is " + record);
				records.add(record);
			});
			if (consumed > 0) {
				totalConsumedRecords += consumed;
			}
		}
		System.out.println("total duration to ingest '" + recordsCount + "' records: " +
				Strings.duration(System.currentTimeMillis() - start));

		try {
			for (int i = 0; i < records.size(); ++i) {
				System.out.println(String.format("VKVK printing record with pk: <id = %d>", i));
				System.out.println(records.get(i)); // definitely not what is being looked for xD
//				getAfterValue(SchemaUtil.asString(records.get(i)));
//			VerifyRecord.print(records.get(i)); // todo vaibhav: this could be a workaround
			}
		} catch (Exception e) {
			System.out.println("Exception caught while parsing records: " + e);
			fail();
		}
	}

	private void verifyValue(long recordsCount) {
		int totalConsumedRecords = 0;
		long start = System.currentTimeMillis();
		List<SourceRecord> records = new ArrayList<>();
		while (totalConsumedRecords < recordsCount) {
			int consumed = super.consumeAvailableRecords(record -> {
				System.out.println("VKVK the record being consumed is " + record);
				records.add(record);
			});
			if (consumed > 0) {
				totalConsumedRecords += consumed;
			}
		}
		System.out.println("total duration to ingest '" + recordsCount + "' records: " +
				Strings.duration(System.currentTimeMillis() - start));

		try {
			for (int i = 0; i < records.size(); ++i) {
				System.out.println(String.format("VKVK verifying record values with pk: <id = %d>", i));
				assertValueField(records.get(i), "after/id", i);
				assertValueField(records.get(i), "after/first_name", "Vaibhav");
				assertValueField(records.get(i), "after/last_name", "Kushwaha");
			}
		} catch (Exception e) {
			System.out.println("Exception caught while parsing records: " + e);
			fail();
		}
	}

	@BeforeClass
	public static void beforeClass() throws SQLException {
		TestHelper.dropAllSchemas();
	}

	@Before
	public void before() {
		initializeConnectorTestFramework();
	}

	@After
	public void after() {
		stopConnector();
	}

	@Test
	public void testConnectorConfig() {
		connector = new YugabyteDBConnector();
		ConfigDef configDef = connector.config();
		assertNotNull(configDef);
	}

	@Test
	public void testSimpleOps() throws Exception {
		TestHelper.dropAllSchemas();
		TestHelper.executeDDL("postgres_create_tables.ddl");
		Thread.sleep(1000); // todo vaibhav: find why this (sleep) is called
		Configuration.Builder configBuilder = getConfigBuilder();
		start(YugabyteDBConnector.class, configBuilder.build());
		assertConnectorIsRunning();
		final long recordsCount = 2;

		// insert rows in the table t1 with values <some-pk, 'Vaibhav', 'Kushwaha', 30>
		insertRecords(recordsCount);
//		 batchInsertRecords(recordsCount, batchSize);
		CompletableFuture.runAsync(() -> consumeRecords(recordsCount))
				.exceptionally(throwable -> {
					throw new RuntimeException(throwable);
				}).get();
	}

	@Test
	public void testLargeLoad() throws Exception {
		TestHelper.dropAllSchemas();
		TestHelper.executeDDL("postgres_create_tables.ddl");
		Thread.sleep(1000); // todo vaibhav: find why this is called
		Configuration.Builder configBuilder = getConfigBuilder();
		start(YugabyteDBConnector.class, configBuilder.build());
		assertConnectorIsRunning();
		final long recordsCount = 1000;

		// insert rows in the table t1 with values <some-pk, 'Vaibhav', 'Kushwaha', 30>
		insertRecords(recordsCount);
		CompletableFuture.runAsync(() -> consumeRecords(recordsCount))
				.exceptionally(throwable -> {
					throw new RuntimeException(throwable);
				}).get();
	}

	// todo vaibhav: remove this test
	@Test
	public void testSample() throws Exception {
		TestHelper.dropAllSchemas();
		TestHelper.executeDDL("postgres_create_tables.ddl");
		Thread.sleep(1000); // todo vaibhav: find why this is called
		Configuration.Builder configBuilder = getConfigBuilder();
		start(YugabyteDBConnector.class, configBuilder.build());
		assertConnectorIsRunning();
		final long recordsCount = 1;
		// final int batchSize = 10;

		// insert rows in the table t1 with values <some-pk, 'Vaibhav', 'Kushwaha', 30>
		insertRecords(recordsCount);
//		 batchInsertRecords(recordsCount, batchSize);
		CompletableFuture.runAsync(() -> printRecordsJson(recordsCount))
				.exceptionally(throwable -> {
					throw new RuntimeException(throwable);
				}).get();
	}

	@Test
	public void testVerifyValue() throws Exception {
		TestHelper.dropAllSchemas();
		TestHelper.executeDDL("postgres_create_tables.ddl");
		Thread.sleep(1000);
		Configuration.Builder configBuilder = getConfigBuilder();
		start(YugabyteDBConnector.class, configBuilder.build());
		assertConnectorIsRunning();
		final long recordsCount = 1;
		// final int batchSize = 10;

		// insert rows in the table t1 with values <some-pk, 'Vaibhav', 'Kushwaha', 30>
		insertRecords(recordsCount);
//		 batchInsertRecords(recordsCount, batchSize);
		CompletableFuture.runAsync(() -> verifyValue(recordsCount))
				.exceptionally(throwable -> {
					throw new RuntimeException(throwable);
				}).get();
	}
}
