package se.primenta.common.persistence;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;

import se.primenta.common.persistence.CassandraExecutor;
import se.primenta.common.persistence.CassandraSession;
import se.primenta.common.persistence.ColumnDefinition;
import se.primenta.common.persistence.test.CassandraTestBase;

public class CassandraSessionTest extends CassandraTestBase {

    private final static String REPLICATION = "{'class': 'SimpleStrategy', 'replication_factor': '1'}";
    private final static String USER = "cassandra";
    private final static String PASSWD = "cassandra";
    private final static String KEYSPACE = "mdr_test";

    @Test
    public void createWithBuilderNoColumnDefiniton() {

        // Create the keyspace first to make sure we have something to connect to.
        new CassandraSession.SessionBuilder("localhost:" + getCassandraPort())
                .forDataCenter("")
                .usingKeyspace(KEYSPACE)
                .andReplication(REPLICATION)
                .asUser(USER)
                .andPassword(PASSWD)
                .preprocessTheseStatements(Collections.emptyList())
                .asPreprocessUserName(USER)
                .andPreprocessPassword(PASSWD)
                .build();

        final CassandraSession session = new CassandraSession.SessionBuilder("localhost:" + getCassandraPort())
                .forDataCenter("")
                .usingKeyspace(KEYSPACE)
                .andReplication(REPLICATION)
                .asUser(USER)
                .andPassword(PASSWD)
                .build();
        final CassandraExecutor executor = session.getExecutor();

        executor.execute(
                new SimpleStatement("create table if not exists testmysession(id int primary key, test text)"));
        executor.execute(new SimpleStatement("insert into testmysession(id, test) values (1, 'hello')"));
        assertEquals(1, executor.execute(new SimpleStatement("select * from testmysession")).all().size());

    }

    @Test
    public void createWithBuilderWithColumnDefinitons() {

        final ColumnDefinition definition1 = new ColumnDefinition() {
            @Override
            public List<Statement> getStatements() {
                return Arrays.asList(
                        new SimpleStatement("create table if not exists testtable1(id int primary key, test text)"));
            }
        };
        final ColumnDefinition definition2 = new ColumnDefinition() {
            @Override
            public List<Statement> getStatements() {
                return Arrays.asList(
                        new SimpleStatement("create table if not exists testtable2(id int primary key, test text)"));
            }
        };

        final CassandraSession session = new CassandraSession.SessionBuilder("localhost:" + getCassandraPort())
                .forDataCenter("")
                .usingKeyspace(KEYSPACE)
                .andReplication(REPLICATION)
                .asUser(USER)
                .andPassword(PASSWD)
                .preprocessTheseStatements(Arrays.asList(definition1, definition2))
                .asPreprocessUserName(USER)
                .andPreprocessPassword(PASSWD)
                .build();
        final CassandraExecutor executor = session.getExecutor();

        executor.execute(new SimpleStatement("insert into testtable1(id, test) values (3, 'worldz')"));
        assertEquals(1, executor.execute(new SimpleStatement("select * from testtable1")).all().size());
        assertEquals("worldz", executor.execute(new SimpleStatement("select * from testtable1")).all().get(0)
                .get("test", String.class));

        executor.execute(new SimpleStatement("insert into testtable2(id, test) values (3, 'war III')"));
        assertEquals(1, executor.execute(new SimpleStatement("select * from testtable2")).all().size());
        assertEquals("war III", executor.execute(new SimpleStatement("select * from testtable2")).all().get(0)
                .get("test", String.class));

    }

    @Test
    public void createWithBuilderWithColumnDefiniton() {

        final ColumnDefinition definition = new ColumnDefinition() {
            @Override
            public List<Statement> getStatements() {
                return Arrays.asList(
                        new SimpleStatement("create table if not exists testwithcreds(id int primary key, test text)"));
            }
        };

        final CassandraSession session = new CassandraSession.SessionBuilder("localhost:" + getCassandraPort())
                .forDataCenter("")
                .usingKeyspace(KEYSPACE)
                .andReplication(REPLICATION)
                .asUser(USER)
                .andPassword(PASSWD)
                .preprocessTheseStatements(Arrays.asList(definition))
                .asPreprocessUserName(USER)
                .andPreprocessPassword(PASSWD)
                .build();
        final CassandraExecutor executor = session.getExecutor();

        executor.execute(new SimpleStatement("insert into testwithcreds(id, test) values (3, 'worldz')"));
        assertEquals("worldz", executor.execute(new SimpleStatement("select * from testwithcreds")).all().get(0)
                .get("test", String.class));

    }

}
