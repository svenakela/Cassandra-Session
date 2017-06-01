[![Build Status](https://travis-ci.org/svenakela/Cassandra-Session.png)](https://travis-ci.org/svenakela/Cassandra-Session)
[![codecov.io](https://codecov.io/github/svenakela/Cassandra-Session/coverage.svg?branch=master)](https://codecov.io/github/svenakela/Cassandra-Session?branch=master)
[![Release](https://jitpack.io/v/svenakela/Cassandra-Session.svg)]
(https://jitpack.io/#svenakela/Cassandra-Session)
[![Open Source Love](https://badges.frapsoft.com/os/mit/mit.svg?v=102)](https://github.com/ellerbrock/open-source-badge/)

[![forthebadge](http://forthebadge.com/badges/gluten-free.svg)](http://forthebadge.com)

# Cassandra Session #

Library wrapping a cassandra driver and takes care of all the configuration and async calls, making your code easier to write and maintain.

## Main Features ##

* Creates a thread safe session
* Checks the keyspace availability during startup
* Run statements before the session is created (create table etc)
* Use different users for pre statements and normal execution
* Handles async operations, with or without Future results
* Predefined Retry Policy
* Fallback to synchronous retries on CAS problems or exceptions
* Automatic handling of a shutdown for a clean close of the session
* Codecs for modern temporal time types already added

## How to use ##

Using the library is pretty straight forward, add a dependency and give the credentials.

### Dependency ###

Add following to the build.gradle:

```Gradle
repositories {
    ...
    maven { url 'https://jitpack.io' }
}

dependencies {
    ...
    compile 'com.github.svenakela:Cassandra-Session:+'
}
```

### Logging ###

Both the library and the underlying driver are using SLF4J for logging.

### Connect ###

To create a session there is a fluent interface Builder that configures and builds the CassandraSession object:

```Java
CassandraSession session = new CassandraSession.SessionBuilder(contactpoints)
                .forDataCenter(datacenter)
                .usingKeyspace(keyspace)
                .andReplication(replication)
                .asUser(user)
                .andPassword(password)
                .preprocessTheseStatements(Arrays.asList(new ColumnFamilies()))
                .asPreprocessUserName(preProcessUser)
                .andPreprocessPassword(preProcessPassword)
                .build();
```

* contactpoints - Nodes to contact in the C* cluster for initial connect
  * seed-server1, seed-serv2, seed-server3
* datacenter - When a cluster is splitted in datacenters, specify which to connect to
  * Not in use (yet) at Tingcore, set an empty String
* keyspace - The application specific keyspace name
* replication - The strategy of replication, which differs if this is a dev, test or prod environment
  * dev - "{'class': 'SimpleStrategy', 'replication_factor': '1'}"
  * test - "{'class': 'SimpleStrategy', 'replication_factor': '3'}"
  * prod - "{'class': 'SimpleStrategy', 'replication_factor': '3'}"
  * Can also be set to NetworkStrategy etc, but not in use at Tingcore
* user - The normal operation user
* password - ...
* preprocess statements - A list of statements that should be executed before we are ready
* preprocesspassword: ....

When the session object is built an Executor can be required. The executor is thread safe as well and can be shared, but several executors can be created too. They will still use the same session in the background.

```Java
private final CassandraExecutor executor = session.getExecutor();
```

### How to use the Executor ###

* Always create PreparedStatements. Like, always. There is no excuse to not.
* Run the statement with either:
  * `execute(final Statement statement)` - Synchronous call that returns a Resultset
  * `executeAsync(final Statement statement)` - Asynchronous call that returns a Resultset as a Future
  * `fireAndForget(final Statement statement)` - Shoot the statement and happily continue with something else. Perfect for insert statements

### Good coding behaviour ###

* Avoid Literal CQL, like a String based statement "insert into ..."
* Use `QueryBuilder` to create statements
* Use a Table class with constants or an enum to avoid repeated string literals in the code

If mapping is preferred a `MappingManager` is created by the CasandraSession and it can be used as well. Please note that by using the mapper all the automatic fail management is not used. This could still be OK for a non performance application. MappingManager and the Mapper created with the manager are thread safe and should be shared within the application.

[How to use the mapper is well documented here](http://docs.datastax.com/en/developer/java-driver/3.2/manual/object_mapper/using/)

### Temporal Time Codecs ###

Codecs for JDK8 Instant, LocalDate and LocalTime are already added to the session and you can store data types as is without formatting in advance. Data types are parsed into following Cassandra data types and these are what the data column should be defined with.

 * Instant - timestamp
 * LocalDate - Date
 * LocalTime - Time
 
 ```Java
 private PreparedStatement stmt = session.prepare(new SimpleStatement("insert into testtime(id, t) values(1, ?)"));
 session.getExecutor().fireAndForget(stmt.bind(Instant.now());
 ```

Please note that these values are non time zoned and you should make sure the values are in UTC. To be able to store time values on the timeline with a time zone you can create a Tuple Data Type [described here](http://docs.datastax.com/en/developer/java-driver/3.2/manual/custom_codecs/extras/#jdk-8).

### Write tests ###

To be able to run tests with a persistence layer the test framework [Cassandra Testbase](https://github.com/svenakela/Cassandra-Testbase) can be used.

To write isolated unit tests for POJO classes it is easy to mock the library. Following example mocks the persistence layer totally (which may not be needed) to verify that the values are prepared for insert.


```Java
@RunWith(MockitoJUnitRunner.class)
public class FailedMessagePersisterTest {

    @Mock
    private EventBus bus;

    @Mock
    private CassandraSession session;

    @Mock
    private CassandraExecutor executor;

    @Mock
    private PreparedStatement prepStmt;

    @Mock
    private BoundStatement boundStmt;

    @Captor
    private ArgumentCaptor<String> captor;

    private static final String MESSAGE = "mdrMessagemdrMessagemdrMessagemdrMessage";
    private static final String ERR = "errorz in da house!";

    @Test
    public void testPersistence() {

        when(session.getExecutor()).thenReturn(executor);
        when(session.prepare(any())).thenReturn(prepStmt);
        when(prepStmt.bind()).thenReturn(boundStmt);
        when(boundStmt.setString(any(), any())).thenReturn(boundStmt);
        when(boundStmt.setTimestamp(any(), any())).thenReturn(boundStmt);

        // Execute the test itself, save a fail message.
        final FailedMdrParsePersister fpp = new FailedMdrParsePersister(session, bus);
        final FailedMdrEvent event = new FailedMdrEvent(MESSAGE, ERR);
        fpp.persist(event);

        // Verify that the message and error string are set in the statement.
        verify(boundStmt, atLeast(2)).setString(any(), captor.capture());
        Assert.assertTrue(captor.getAllValues().contains(MESSAGE));
        Assert.assertTrue(captor.getAllValues().contains(ERR));
    }
}
```
