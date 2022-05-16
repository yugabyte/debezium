package io.debezium.connector.yugabytedb;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.YugabyteYSQLContainer;
// import org.testcontainers.utility.DockerImageName;

import io.debezium.embedded.AbstractConnectorTest;

public class YugabyteDBTestContainersTest extends AbstractConnectorTest {
  private static YugabyteYSQLContainer ybContainer;

    @BeforeClass
    public static void beforeClass() throws SQLException {
        ybContainer = new YugabyteYSQLContainer("yugabytedb/yugabyte:2.13.1.0-b112");
        ybContainer.addExposedPorts(7100, 9100);
        System.out.println("Going to start the container");
        ybContainer.start();
        // TestHelper.dropAllSchemas();
    }

    @AfterClass
    public static void afterClass() throws SQLException {
      ybContainer.close();
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
    public void startTestContainers() throws SQLException {
      System.out.println("Starting the test (hoping the DB would be initialized)");

      ResultSet rs = TestHelper.performQuery(ybContainer, "SELECT yb_version()");

      String val = rs.getString(1);

      System.out.println("Value: " + val);
    }
}
