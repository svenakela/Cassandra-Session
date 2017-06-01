package com.tingcore.common.persistence;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.LoggingRetryPolicy;
import com.datastax.driver.extras.codecs.jdk8.InstantCodec;
import com.datastax.driver.extras.codecs.jdk8.LocalDateCodec;
import com.datastax.driver.extras.codecs.jdk8.LocalTimeCodec;
import com.datastax.driver.mapping.MappingManager;

/**
 * The core implementation of the {@link CassandraSession} interface.
 *
 * @author Sven Wesley
 *
 */
public final class CassandraSessionImpl implements CassandraSession {

    private final String keyspace;
    private final String nodes;
    private final String replication;
    private final Optional<String> datacenter;
    private final Session sessionSingleton;
    private final MappingManager mappingManager;

    private static final Logger LOGGER = LogManager.getLogger(CassandraSessionImpl.class);
    private static final Marker CASSANDRA = MarkerManager.getMarker("CASSANDRA_SESSION");

    private static final String CREATE_KEYSPACE = "CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = %s";

    /**
     * This is where all the magic happens.
     */
    protected CassandraSessionImpl(final String user,
            final String password,
            final String keyspace,
            final String nodes,
            final String replication,
            final Optional<String> datacenter,
            final Optional<String> preuser,
            final Optional<String> prepassword,
            final List<? extends ColumnDefinition> definitions) {

        this.keyspace = keyspace;
        this.nodes = nodes;
        this.replication = replication;
        this.datacenter = datacenter;

        if (preuser.isPresent()) {

            final Cluster cluster = createCluster(preuser.get(), prepassword.get());

            // Make sure that the keyspace exists before we default to it, ugly double connect but must be created
            // first before it can be used as a default keyspace.
            final Session keyspaceSession = cluster.connect();
            ensureKeyspace(keyspaceSession);
            keyspaceSession.close();

            // Make sure tables exist in the give given default keyspace
            final Session createSession = cluster.connect(keyspace);
            ensureColumnfamilies(createSession, definitions);
            createSession.close();
        }

        sessionSingleton = createCluster(user, password).connect(keyspace);
        mappingManager = new MappingManager(sessionSingleton);

        // Lets be modern and add temporal codecs right away
        sessionSingleton.getCluster().getConfiguration().getCodecRegistry().register(
                InstantCodec.instance, LocalDateCodec.instance, LocalTimeCodec.instance
        );

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                LOGGER.info(CASSANDRA, "Graceful session close is initiated");
                sessionSingleton.close();
            }
        });
    }

    @Override
    public PreparedStatement prepare(final RegularStatement statement) {
        return sessionSingleton.prepare(statement);
    }

    @Override
    public CassandraExecutor getExecutor() {
        return new CassandraExecutorImpl(sessionSingleton);
    }

    @Override
    public MappingManager getMappingManager() {
        return mappingManager;
    }

    @Override
    public void registerCodec(final TypeCodec<?> codec) {
        sessionSingleton.getCluster().getConfiguration().getCodecRegistry().register(codec);
    }

    @Override
    public TupleType createTupleType(final DataType a, final DataType b) {
        return sessionSingleton.getCluster().getMetadata().newTupleType(a, b);
    }

    private Cluster createCluster(final String user, final String password) {

        LOGGER.info(CASSANDRA, "Creating session for {}, replication factor {}, datacenter {}", nodes, replication,
                datacenter);

        final Cluster.Builder builder = Cluster.builder()
                .withCredentials(user, password)
                .withRetryPolicy(new LoggingRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE));

        datacenter.ifPresent(dc -> {
            final DCAwareRoundRobinPolicy.Builder dcAwareBuilder = DCAwareRoundRobinPolicy.builder();
            dcAwareBuilder.withLocalDc(dc);
            final DCAwareRoundRobinPolicy dcAwarePolicy = dcAwareBuilder.build();
            builder.withLoadBalancingPolicy(dcAwarePolicy);
        });

        Arrays.stream(nodes.split(",")).map(x -> x.split(":")).forEach(y -> {
            builder.addContactPoint(y[0].trim());
            if (y.length == 2) {
                builder.withPort(Integer.parseInt(y[1]));
            }
        });

        return builder.build();
    }

    private void ensureKeyspace(final Session sess) {

        LOGGER.info(CASSANDRA, "Ensuring keyspace");
        try {
            sess.execute(String.format(CREATE_KEYSPACE, keyspace, replication));
        } catch (QueryValidationException | QueryExecutionException e) {
            LOGGER.error(CASSANDRA, "Failed to create keyspace", e);
            throw new PersistenceRuntimeException(e);
        }
    }

    private void ensureColumnfamilies(final Session sess, final List<? extends ColumnDefinition> definitions) {

        LOGGER.info(CASSANDRA, "Executing table definitions");
        try {
            definitions.stream().map(ColumnDefinition::getStatements).flatMap(List::stream).forEach(sess::execute);
        } catch (QueryValidationException | QueryExecutionException e) {
            LOGGER.error(CASSANDRA, "Failed to create column families from definition", e);
            throw new PersistenceRuntimeException(e);
        }
    }

}