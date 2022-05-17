package org.testcontainers.junit.oracle;

import org.junit.Rule;
import org.testcontainers.containers.OracleADBContainer;
import org.testcontainers.db.AbstractContainerDatabaseTest;
import org.testcontainers.utility.DockerImageName;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SimpleOracleADBTest extends AbstractContainerDatabaseTest {

    public static final DockerImageName ORACLE_DOCKER_IMAGE_NAME = DockerImageName.parse("loiclefevre/oracle-adb:19.0.0");

    @Rule
    public final OracleADBContainer oracle = new OracleADBContainer(ORACLE_DOCKER_IMAGE_NAME)
        .withDatabaseName("AJDSAI2").withFreeTiers(false).withProfile("DEFAULT-MARSEILLE").withWorkloadType("oltp").withReuse(true).withUsernamePrefix("tc_test_");

    public void runTest(OracleADBContainer container, String databaseName, String username, String password) throws SQLException {

        //Test config was honored
        assertEquals(databaseName, container.getDatabaseName());
        assertTrue(container.getUsername().startsWith("tc_test_"));
        assertEquals(password, container.getPassword());

        //Test we can get a connection
        container.start();
        ResultSet resultSet = performQuery(container, "SELECT 1 FROM dual");
        int resultSetInt = resultSet.getInt(1);
        assertEquals("A basic SELECT query succeeds", 1, resultSetInt);
    }
}
