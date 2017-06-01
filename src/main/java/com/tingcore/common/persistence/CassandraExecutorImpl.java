package com.tingcore.common.persistence;

import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public final class CassandraExecutorImpl implements CassandraExecutor {

    final static int MAX_STORE_RETRIES = 3;

    private final Session session;

    private static final Logger LOGGER = LogManager.getLogger(CassandraSessionImpl.class);
    private static final Marker EXEC = MarkerManager.getMarker("CASSANDRA_EXECUTOR");

    private final ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());

    protected CassandraExecutorImpl(final Session session) {
        this.session = session;
    }

    @Override
    public void fireAndForget(final Statement statement) {

        final ResultSetFuture future = session.executeAsync(statement);
        Futures.addCallback(future, new FutureCallback<ResultSet>() {

            @Override
            public void onSuccess(final ResultSet result) {
                // Do nothing.
            }

            @Override
            public void onFailure(final Throwable t) {

                if (t instanceof QueryValidationException) {
                    LOGGER.error(EXEC, "Fire and Forget failed. {}", statement.toString(), t);
                } else {
                    LOGGER.error(EXEC, "Async persist failed for {}, {}. Backing off to a serial execution",
                            statement.toString(), t);
                    retryExecute(statement);
                }
            }
        }, executor);
    }

    @Override
    public ResultSet execute(final Statement statement) {
        return retryExecute(statement);
    }

    @Override
    public ResultSetFuture executeAsync(final Statement statement) {
        return session.executeAsync(statement);
    }

    /**
     * Private execute method to handle CAS operations. Is used by the async method as a retry operation.
     *
     * @param statement
     * @return Resultset of the query
     */
    private ResultSet retryExecute(final Statement statement) {

        int tries = 0;
        ResultSet result = null;
        while (tries <= MAX_STORE_RETRIES) {
            tries++;
            try {
                result = session.execute(statement);
                break;
            } catch (final WriteTimeoutException wte) {
                // The WriteTimeOutException will occur if CAS operations fail.
                // It will not be handled by the DefaultPolicy and we need to retry manually.
                // But lets not care what type of timeout reason there is, lets retry anyway.
                if (tries < MAX_STORE_RETRIES) {
                    LOGGER.warn(EXEC, "Storing data failed with " + wte.getWriteType() + " problems, will retry.");
                } else {
                    throw new PersistenceRuntimeException("Timeout problems when storing data.", wte);
                }
            }
        }
        return result; // Will actually never be null. If it is the session is dead.
    }

}
