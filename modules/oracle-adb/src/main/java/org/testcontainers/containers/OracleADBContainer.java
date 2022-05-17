package org.testcontainers.containers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.model.Frame;
import org.apache.commons.lang3.StringUtils;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.testcontainers.UnstableAPI;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.time.temporal.ChronoUnit.SECONDS;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Oracle Autonomous Database Container.
 */
public class OracleADBContainer extends JdbcDatabaseContainer<OracleADBContainer> {

    private static final Logger LOGGER = getLogger(OracleADBContainer.class);
    private static final UUID JVM_ID = UUID.randomUUID();

    public static final String NAME = "oracleADB";
    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("loiclefevre/oracle-adb");

    static final String DEFAULT_TAG = "19.0.0";
    static final String IMAGE = DEFAULT_IMAGE_NAME.getUnversionedPart();

    private static final int DEFAULT_STARTUP_TIMEOUT_SECONDS = 120;
    private static final int DEFAULT_CONNECT_TIMEOUT_SECONDS = 60;

    // Container defaults
    static final String DEFAULT_DATABASE_NAME = "adb1";
    static final String DEFAULT_WORKLOAD_TYPE = "oltp";
    static final String DEFAULT_PROFILE = "DEFAULT";

    // Test container defaults
    static final String APP_USER = "test";
    static final String APP_USER_PASSWORD = "C0mplex_Passw0rd";

    // Restricted user names
    private static final List<String> ORACLE_MANAGED_USERS = Arrays.asList(
        "acchk_read", "adm_parallel_execute_task", "anonymous", "application_trace_viewer", "appqossys",
        "aq_administrator_role", "aq_user_role", "audit_admin", "audit_viewer", "audsys", "authenticateduser",
        "avtune_pkg_role", "bdsql_admin", "bdsql_user", "capture_admin", "cdb_dba", "connect", "ctxapp", "ctxsys",
        "datapatch_role", "datapump_exp_full_database", "datapump_imp_full_database", "dba", "dbfs_role",
        "dbms_mdx_internal", "dbsfwuser", "dbsnmp", "dgpdb_int", "dip", "dvf", "dvsys", "dv_acctmgr", "dv_admin",
        "dv_audit_cleanup", "dv_datapump_network_link", "dv_goldengate_admin", "dv_goldengate_redo_access",
        "dv_monitor", "dv_owner", "dv_patch_admin", "dv_policy_owner", "dv_secanalyst", "dv_streams_admin",
        "dv_xstream_admin", "em_express_all", "em_express_basic", "execute_catalog_role", "exp_full_database",
        "gather_system_statistics", "gds_catalog_select", "ggsys", "ggsys_role", "global_aq_user_role",
        "gsmadmin_internal", "gsmadmin_role", "gsmcatuser", "gsmrootuser", "gsmrootuser_role", "gsmuser",
        "gsmuser_role", "gsm_pooladmin_role", "hs_admin_execute_role", "hs_admin_role", "hs_admin_select_role",
        "imp_full_database", "lbacsys", "lbac_dba", "logstdby_administrator", "maintplan_app", "mddata", "mdsys",
        "oem_advisor", "oem_monitor", "olap_xs_admin", "optimizer_processing_rate", "oracle_ocm", "outln", "pdb_dba",
        "pplb_role", "provisioner", "rdfctx_admin", "recovery_catalog_owner", "recovery_catalog_owner_vpd",
        "recovery_catalog_user", "remote_scheduler_agent", "resource", "scheduler_admin", "select_catalog_role",
        "soda_app", "sys", "sys$umf", "sysbackup", "sysdg", "syskm", "sysrac", "system", "sysumf_role", "xdb", "xdbadmin",
        "xdb_set_invoker", "xdb_webservices", "xdb_webservices_over_http", "xdb_webservices_with_public", "xs$null",
        "xs_cache_admin", "xs_connect", "xs_namespace_admin", "xs_session_admin"
    );

    private String databaseName = DEFAULT_DATABASE_NAME;
    private String username = APP_USER;
    private String usernamePrefix = null;
    private String password = APP_USER_PASSWORD;
    private String adminPassword = APP_USER_PASSWORD;

    private String workloadType = DEFAULT_WORKLOAD_TYPE;
    private String profile = DEFAULT_PROFILE;
    private boolean freeTiers = true;

    private File databaseConfigurationFile;
    private ADBConfiguration adbConfiguration;
    private boolean shouldBeReused = false;

    public OracleADBContainer(final DockerImageName dockerImageName) {
        super(dockerImageName);
        dockerImageName.assertCompatibleWith(DEFAULT_IMAGE_NAME);
        preconfigure();
    }

    private void preconfigure() {
        this.waitStrategy = new LogMessageWaitStrategy()
            .withRegEx(".*DATABASE IS READY TO USE!.*\\s")
            .withTimes(1)
            .withStartupTimeout(Duration.of(DEFAULT_STARTUP_TIMEOUT_SECONDS, SECONDS));

        withConnectTimeoutSeconds(DEFAULT_CONNECT_TIMEOUT_SECONDS);
    }

    @UnstableAPI
    public OracleADBContainer withReuse(boolean reusable) {
        this.shouldBeReused = reusable;
        return super.withReuse(reusable);
     //   return super.withReuse(false);
    }

    @Override
    protected void waitUntilContainerStarted() {
        getWaitStrategy().waitUntilReady(this);
    }

    @Override
    public String getDriverClassName() {
        return "oracle.jdbc.OracleDriver";
    }

    @Override
    public String getJdbcUrl() {
        return "jdbc:oracle:thin:@" + getADBConfiguration().getConnectionString() +
            "?oracle.jdbc.fanEnabled=false&oracle.jdbc.thinForceDNSLoadBalancing=true";
    }

    private ADBConfiguration getADBConfiguration() {
        if (adbConfiguration != null) {
            return adbConfiguration;
        }
        else {
            try {
                return adbConfiguration = new ObjectMapper().readValue(databaseConfigurationFile, ADBConfiguration.class);
            }
            catch (IOException ioe) {
                throw new ContainerLaunchException("Failed to retrieve JDBC configuration!", ioe);
            }
        }
    }

    @Override
    public String getUsername() {
        return usernamePrefix != null ? usernamePrefix + getUniqueUserId() : username;
    }

    @Override
    public String getPassword() {
        return password;
    }

    public String getAdminPassword() {
        return adminPassword;
    }

    @Override
    public String getDatabaseName() {
        return databaseName;
    }

    public String getWorkloadType() {
        return workloadType;
    }

    public String getProfile() {
        return profile;
    }

    public boolean isFreeTiers() {
        return freeTiers;
    }

    @Override
    public String getTestQueryString() {
        return "SELECT 1 FROM DUAL";
    }

    @Override
    public OracleADBContainer withUsername(String username) {
        if (StringUtils.isEmpty(username)) {
            throw new IllegalArgumentException("Username cannot be null or empty");
        }
        if (ORACLE_MANAGED_USERS.contains(username.toLowerCase())) {
            throw new IllegalArgumentException("Username cannot be one of " + ORACLE_MANAGED_USERS);
        }
        this.username = username;
        return self();
    }

    public OracleADBContainer withUsernamePrefix(String usernamePrefix) {
        if (StringUtils.isEmpty(usernamePrefix)) {
            throw new IllegalArgumentException("Username prefix cannot be null or empty");
        }
        if (usernamePrefix.length() > 9) {
            throw new IllegalArgumentException("Username prefix cannot be longer than 9 chars");
        }
        this.usernamePrefix = usernamePrefix;
        return self();
    }

    @Override
    public OracleADBContainer withPassword(String password) {
        if (StringUtils.isEmpty(password)) {
            throw new IllegalArgumentException("Password cannot be null or empty");
        }
        this.password = password;
        return self();
    }

    public OracleADBContainer withAdminPassword(String adminPassword) {
        if (StringUtils.isEmpty(adminPassword)) {
            throw new IllegalArgumentException("ADMIN password cannot be null or empty");
        }
        this.adminPassword = adminPassword;
        return self();
    }

    @Override
    public OracleADBContainer withDatabaseName(String databaseName) {
        if (StringUtils.isEmpty(databaseName)) {
            throw new IllegalArgumentException("Database name cannot be null or empty");
        }
        if (DEFAULT_DATABASE_NAME.equalsIgnoreCase(databaseName)) {
            throw new IllegalArgumentException("Database name cannot be set to " + DEFAULT_DATABASE_NAME);
        }
        this.databaseName = databaseName;
        return self();
    }

    public OracleADBContainer withProfile(String profile) {
        if (StringUtils.isEmpty(profile)) {
            throw new IllegalArgumentException("Profile cannot be null or empty");
        }
        this.profile = profile;
        return self();
    }

    public OracleADBContainer withFreeTiers(boolean freeTiers) {
        this.freeTiers = freeTiers;
        return self();
    }

    public OracleADBContainer withWorkloadType(String workloadType) {
        if (StringUtils.isEmpty(workloadType)) {
            throw new IllegalArgumentException("Workload type cannot be null or empty");
        }

        switch (workloadType.toLowerCase()) {
            case "json":
            case "oltp":
            case "dw":
            case "apex":
                this.workloadType = workloadType;
                return self();

            default:
                throw new IllegalArgumentException("Workload type cannot be set to " + workloadType + ", use either: json, oltp, dw, apex");

        }
    }

    private final Map<String, String> uniqueIdsMap = new HashMap<>();

    // Unique ID in case of parallel JUnit runners per JVM
    static int UNIQUE_USER_ID = 1;

    private synchronized String getUniqueUserId() {
        final String userKey = String.format("%s_%d", SchemaUUID.get(JVM_ID), Thread.currentThread().getId());

        if (uniqueIdsMap.containsKey(userKey)) {
            return uniqueIdsMap.get(userKey);
        }

        final String uniqueId = String.format("%s_%d", userKey, UNIQUE_USER_ID++);

        uniqueIdsMap.put(userKey, uniqueId);

        return uniqueId;
    }

    @Override
    public void starting(Description description) {
    }

    @Override
    public void finished(Description description) {
        this.stop();
    }

    @Override
    public void start() {

        // overload USER environment variable just in time (after configure())...
        // getUsername() already takes into account the unique identifier!
        if (usernamePrefix == null) {
            withEnv("USER", getUsername());
        }
        else {
            withEnv("USER", "sharedDatabase");
        }

        super.start(); // will call configure()

        // from now, the container is started
        // - create user
        if (usernamePrefix != null) {
            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            try {
                getDockerClient()
                    .execStartCmd(dockerClient.execCreateCmd(getContainerId()).withAttachStdout(true)
                        .withCmd("dragonlite", "-cu", "-p", getProfile(), "-ap", getAdminPassword(),
                            "-u", usernamePrefix + getUniqueUserId(), "-up", getPassword()
                        ).exec().getId())
                    .exec(new ResultCallback.Adapter<Frame>() {
                        @Override
                        public void onNext(Frame frame) {
                            if (frame == null) {
                                return;
                            }
                            switch (frame.getStreamType()) {
                                case RAW:
                                case STDOUT:
                                    try {
                                        outputStream.write(frame.getPayload());
                                        outputStream.flush();
                                    }
                                    catch (IOException e) {
                                        onError(e);
                                    }
                            }
                        }
                    })
                    .awaitCompletion();
            }
            catch (Exception e) {
                LOGGER.debug("Can't exec create user command", e);
            }
        }
    }


    @Override
    protected void configure() {
        withEnv("IP_ADDRESS", PublicIPv4Retriever.get());
        withEnv("REUSE", String.valueOf(shouldBeReused));
        withEnv("PROFILE_NAME", getProfile());
        withEnv("WORKLOAD_TYPE", getWorkloadType());
        withEnv("DATABASE_NAME", getDatabaseName());
        withEnv("ADMIN_PASSWORD", getAdminPassword());
        withEnv("USER_PASSWORD", getPassword());
        withEnv("FREE_TIERS", String.valueOf(isFreeTiers()));

        try {
            databaseConfigurationFile = new File(System.getProperty("java.io.tmpdir"), getDatabaseName() + ".json");
            databaseConfigurationFile.createNewFile();

            withFileSystemBind(databaseConfigurationFile.getAbsolutePath(), "/opt/oracle/database.json", BindMode.READ_WRITE);
        }
        catch (IOException ioe) {
            throw new ContainerLaunchException("Can't map database.json file!", ioe);
        }

        // Now mapping OCI configuration files
        try {
            final String pathToOCIConfigFile = System.getProperty("user.home") + "/.oci/config";
            final String keyFile = getKeyFilePath(pathToOCIConfigFile, getProfile(), StandardCharsets.UTF_8);

            if (!new File(keyFile).exists()) {
                throw new FileNotFoundException("Could not find private key (key_file): " + keyFile);
            }

            withFileSystemBind(keyFile, "/opt/oracle/key", BindMode.READ_ONLY);
            withFileSystemBind(pathToOCIConfigFile, "/opt/oracle/config", BindMode.READ_ONLY);
        }
        catch (IOException ioe) {
            throw new ContainerLaunchException("Can't map key_file from OCI configuration to container file!", ioe);
        }
    }

    @Override
    public void stop() {
        if (!shouldBeReused) {
            // will fire adb instance termination
            getDockerClient()
                .killContainerCmd(getContainerId())
                .withSignal("SIGTERM")
                .exec();

            try {
                // let some time to launch instance termination
                Thread.sleep(20 * 1000L);
            }
            catch (InterruptedException ignored) {
            }

            super.stop();
        }
        else {
            if (usernamePrefix != null) {
                final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

                try {
                    getDockerClient()
                        .execStartCmd(dockerClient.execCreateCmd(getContainerId()).withAttachStdout(true)
                            .withCmd("dragonlite", "-du", "-p", getProfile(), "-ap", getAdminPassword(),
                                "-u", usernamePrefix + getUniqueUserId(), "-up", getPassword()
                            ).exec().getId())
                        .exec(new ResultCallback.Adapter<Frame>() {
                            @Override
                            public void onNext(Frame frame) {
                                if (frame == null) {
                                    return;
                                }
                                switch (frame.getStreamType()) {
                                    case RAW:
                                    case STDOUT:
                                        try {
                                            outputStream.write(frame.getPayload());
                                            outputStream.flush();
                                        }
                                        catch (IOException e) {
                                            onError(e);
                                        }
                                }
                            }
                        })
                        .awaitCompletion();
                }
                catch (Exception e) {
                    LOGGER.debug("Can't exec delete user command", e);
                }

                final String userKey = String.format("%s_%d", SchemaUUID.get(JVM_ID), Thread.currentThread().getId());
                uniqueIdsMap.remove(userKey);
            }
        }
    }

    private String getKeyFilePath(String pathToOCIConfigFile, String profile, Charset charset) throws IOException {
        final ConfigAccumulator accumulator = new ConfigAccumulator();
        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(pathToOCIConfigFile), charset))) {
            String line;
            while ((line = reader.readLine()) != null) {
                accumulator.accept(line);
            }
        }
        if (!accumulator.foundDefaultProfile) {
        }
        if (profile != null && !accumulator.configurationsByProfile.containsKey(profile)) {
            throw new IllegalArgumentException("No profile named " + profile + " exists in the configuration file");
        }

        if (accumulator.configurationsByProfile.get(profile).containsKey("key_file")) {
            return accumulator.configurationsByProfile.get(profile).get("key_file");
        }
        else {
            throw new IllegalArgumentException("No key_file property found in configuration for profile named " + profile);
        }
    }

    private static final class ConfigAccumulator {
        final Map<String, Map<String, String>> configurationsByProfile = new HashMap<>();

        private String currentProfile = null;
        private boolean foundDefaultProfile = false;

        private void accept(String line) {
            final String trimmedLine = line.trim();

            // no blank lines
            if (trimmedLine.isEmpty()) {
                return;
            }

            // skip comments
            if (trimmedLine.charAt(0) == '#') {
                return;
            }

            if (trimmedLine.charAt(0) == '[' && trimmedLine.charAt(trimmedLine.length() - 1) == ']') {
                currentProfile = trimmedLine.substring(1, trimmedLine.length() - 1).trim();
                if (currentProfile.isEmpty()) {
                    throw new IllegalStateException("Cannot have empty profile name: " + line);
                }
                if (currentProfile.equals("DEFAULT")) {
                    foundDefaultProfile = true;
                }
                if (!configurationsByProfile.containsKey(currentProfile)) {
                    configurationsByProfile.put(currentProfile, new HashMap<>());
                }

                return;
            }

            final int splitIndex = trimmedLine.indexOf('=');
            if (splitIndex == -1) {
                throw new IllegalStateException("Found line with no key-value pair: " + line);
            }

            final String key = trimmedLine.substring(0, splitIndex).trim();
            final String value = trimmedLine.substring(splitIndex + 1).trim();
            if (key.isEmpty()) {
                throw new IllegalStateException("Found line with no key: " + line);
            }

            if (currentProfile == null) {
                throw new IllegalStateException("Config parse error, attempted to read configuration without specifying a profile: " + line);
            }

            configurationsByProfile.get(currentProfile).put(key, value);
        }
    }

    /**
     * ADB configuration POJO.
     */
    private static final class ADBConfiguration {
        private String connectionString;
        private String sqlDevWebUrl;

        public ADBConfiguration() {
        }

        public String getConnectionString() {
            return connectionString;
        }

        public void setConnectionString(String connectionString) {
            this.connectionString = connectionString;
        }

        public String getSqlDevWebUrl() {
            return sqlDevWebUrl;
        }

        public void setSqlDevWebUrl(String sqlDevWebUrl) {
            this.sqlDevWebUrl = sqlDevWebUrl;
        }
    }
}
