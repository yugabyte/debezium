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
        ybContainer = TestHelper.getYbContainer();
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
        System.out.println("Container Id: " + ybContainer.getContainerId());
        System.out.println("Container IP: " + ybContainer.getContainerIpAddress());
        System.out.println("Container: " + ybContainer.getJdbcUrl());

        ResultSet rs = TestHelper.performQuery(ybContainer, "SELECT version()");

        String val = rs.getString(1);

        System.out.println("Value: " + val);
    }
}
