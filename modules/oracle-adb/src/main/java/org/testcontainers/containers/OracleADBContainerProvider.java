package org.testcontainers.containers;

import org.testcontainers.utility.DockerImageName;

/**
 * Factory for Oracle containers.
 */
public class OracleADBContainerProvider extends JdbcDatabaseContainerProvider {
    @Override
    public boolean supports(String databaseType) {
        return databaseType.equals(OracleADBContainer.NAME);
    }

    @Override
    public JdbcDatabaseContainer newInstance() {
        return newInstance(OracleADBContainer.DEFAULT_TAG);
    }

    @Override
    public JdbcDatabaseContainer newInstance(String tag) {
        if (tag != null) {
            return new OracleADBContainer(DockerImageName.parse(OracleADBContainer.IMAGE).withTag(tag));
        }
        return newInstance();
    }
}

