package se.primenta.common.persistence;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Statement;

public interface CassandraExecutor {

    final static int MAX_STORE_RETRIES = 3;

    /**
     * Execute a statement and let the executor take care of all the problems that might occur. Only statements are
     * accepted in this executor. Do not use string based queries, they are bad for your karma.
     *
     * @param statement
     *            query as a statement to execute. The implementation takes care of problems automatically.
     * @return Resultset as a Future that can be ignored, it is only here for testing purposes.
     */
    void fireAndForget(final Statement statement);
    /**
     * Execute a statement synchronously. Use Prepare Statements {@link BoundStatement} to execute queries. Always.
     *
     * @param statement
     * @return a result set as a response for the query.
     */
    ResultSet execute(final Statement statement);

    /**
     * Execute a statement and take care of the async operation.
     *
     * @param statement
     * @return Resultset as a Future that can be acted upon.
     */
    ResultSetFuture executeAsync(final Statement statement);

}
