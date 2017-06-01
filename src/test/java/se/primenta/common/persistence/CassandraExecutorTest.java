package se.primenta.common.persistence;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import se.primenta.common.persistence.CassandraExecutor;
import se.primenta.common.persistence.CassandraSession;
import se.primenta.common.persistence.test.CassandraTestBase;

public class CassandraExecutorTest extends CassandraTestBase {

    static CassandraSession session;

    private final static int SLEEP = 75;

    @BeforeClass
    public static void init() {

        session = new CassandraSession.SessionBuilder("localhost:" + getCassandraPort())
                .usingKeyspace("executortest")
                .andReplication("{'class': 'SimpleStrategy', 'replication_factor': '1'}")
                .asUser("cassandra")
                .andPassword("cassandra")
                .preprocessTheseStatements(Collections.emptyList())
                .asPreprocessUserName("cassandra")
                .andPreprocessPassword("cassandra")
                .build();

        session.getExecutor().execute(new SimpleStatement("create table testexecutor(id int primary key, test text)"));
    }

    @Test
    public void executeSync() {

        final CassandraExecutor exec = session.getExecutor();

        assertEquals(0, exec.execute(new SimpleStatement("select * from testexecutor where id = 666")).all().size());
        assertEquals(0,
                exec.execute(new SimpleStatement("insert into testexecutor(id, test) values (1, 'x')")).all().size());
    }

    @Test
    public void executeAsync() throws InterruptedException, ExecutionException {

        final CassandraExecutor exec = session.getExecutor();
        final ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());

        final ResultSetFuture future = exec
                .executeAsync(new SimpleStatement("insert into testexecutor(id, test) values (2, 'x')"));
        Futures.addCallback(future, new FutureCallback<ResultSet>() {

            @Override
            public void onSuccess(final ResultSet result) {
                assertEquals(1, exec.execute(new SimpleStatement("select * from testexecutor where id = 2"))
                        .all().size());
            }

            @Override
            public void onFailure(final Throwable t) {
                assertTrue(false);
            }
        }, executor);
        future.get();
    }

    /**
     * Testing the ability to catch certain types of exceptions. This is how the fire and forget is checking for errors.
     *
     * @throws InterruptedException
     * @throws ExecutionException
     */
    @Test
    public void executeAsyncFailed() throws InterruptedException, ExecutionException {

        final CassandraExecutor exec = session.getExecutor();
        final ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
        final Map<String, String> failMap = new HashMap<>();
        final ResultSetFuture future = exec
                .executeAsync(new SimpleStatement("insert into testexecutor(id, test) values (2, 666)"));
        Futures.addCallback(future, new FutureCallback<ResultSet>() {

            @Override
            public void onSuccess(final ResultSet result) {
                // No.
            }

            @Override
            public void onFailure(final Throwable t) {
                if (t instanceof QueryValidationException) {
                    failMap.put("FAIL", "GOAT");
                }
            }
        }, executor);

        Thread.sleep(SLEEP);
        assertEquals(1, failMap.size());
    }

    @Test
    public void fireAndForget() throws InterruptedException, ExecutionException {

        final CassandraExecutor exec = session.getExecutor();
        exec.fireAndForget(new SimpleStatement("insert into testexecutor(id, test) values (123, 'x')"));
        // Lets see if this poor bastard has been inserted. Ugly sleep but we need to wait...
        Thread.sleep(SLEEP);
        assertEquals(1, exec.execute(new SimpleStatement("select * from testexecutor where id = 123")).all().size());

    }

    @Test
    public void checkInstantCodec() throws InterruptedException, ExecutionException {

        final CassandraExecutor exec = session.getExecutor();
        exec.execute(new SimpleStatement("create table testtime(id int primary key, t timestamp)"));
        final Instant instant = Instant.now();
        Thread.sleep(SLEEP);
        exec.execute(session.prepare(new SimpleStatement("insert into testtime(id, t) values(1, ?)")).bind(instant));
        Thread.sleep(SLEEP);
        assertEquals(instant, exec.execute(new SimpleStatement("select t from testtime where id = 1")).one()
                .getTimestamp(0).toInstant());
    }
}
