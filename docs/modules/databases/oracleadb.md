# Oracle-ADB Module

See [Database containers](./index.md) for documentation and usage that is common to all relational database container types.

## Usage example

Running Oracle ADB as a stand-in for in a test:

```java
public class SomeTest {

    @Rule
    public OracleADBContainer oracle = new OracleADBContainer("name_of_your_oracle_adb_image");
    
    @Test
    public void someTestMethod() {
        String url = oracle.getJdbcUrl();

        ... create a connection and run test as normal
```

## Specifying a docker image name via config

If you do not pass an image name to the `OracleADBContainer` constructor, a suitable image name should be placed in
configuration instead.
To do this, please place a file on the classpath named `testcontainers.properties`,
containing `oracleadb.container.image=IMAGE`, where IMAGE is a suitable image name and tag.

## Adding this module to your project dependencies

Add the following dependency to your `pom.xml`/`build.gradle` file:

=== "Gradle"
    ```groovy
    testImplementation "org.testcontainers:oracle-adb:{{latest_version}}"
    ```
=== "Maven"
    ```xml
    <dependency>
        <groupId>org.testcontainers</groupId>
        <artifactId>oracle-adb</artifactId>
        <version>{{latest_version}}</version>
        <scope>test</scope>
    </dependency>
    ```

!!! hint
    Adding this Testcontainers library JAR will not automatically add a database driver JAR to your project. You should ensure that your project also has a suitable database driver as a dependency.


