package com.tingcore.common.persistence;

import java.util.List;

import com.datastax.driver.core.Statement;

/**
 * Implement this interface to be able to execute queries in advance when a {@link CassandraSession} is created.
 *
 * @author Sven Wesley
 *
 */
public interface ColumnDefinition {

    /**
     * Return a list of all statements that shall be executed during the preprocess stage.
     *
     * @return list of all CQLs for execution.
     */
    List<Statement> getStatements();

}
