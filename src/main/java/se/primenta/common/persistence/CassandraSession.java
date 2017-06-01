package se.primenta.common.persistence;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.mapping.MappingManager;

/**
 * Cassandra session management and keyspace handler.
 *
 * @author Sven Wesley
 *
 */
public interface CassandraSession {

    /**
     * Prepare a statement as a {@link PreparedStatement} for faster execution. It is highly recommended to always use
     * prepared statements.
     *
     * @param Statement
     * @return a prepared statement that can be bound with parameters multiple times.
     */
    public PreparedStatement prepare(final RegularStatement statement);

    /**
     * Gives you an executing component for executing queries. Uses the thread safe Cassandra session and wraps in
     * features for automatic handling of possible problems taking care of your head ache for you. It also wraps the
     * session and hides methods that should not be used in a high performance application.
     *
     * @return An executor object that should be reused in the entire life cycle of the application.
     */
    public CassandraExecutor getExecutor();

    /**
     * Gives a Mapping Manager that can be used to create automatic mapping of objects. Please note that by using the
     * mapper all the automatic fail management is not used. This could still be ok for a non performant application.
     * MappingManager and the Mapper created with the manager are thread safe.
     *
     * @return MappingManager that can be used for object mapping.
     */
    public MappingManager getMappingManager();

    /**
     * Register a new Type Codec to be able to transform from a C* data type to a Java data type. The session already
     * includes new generation temporal codecs (Instant, LocalDate etc) but there are more custom codecs in the
     * custom_codec/extras library.
     *
     * @param codec
     */
    public void registerCodec(TypeCodec<?> codec);

    /**
     * Create a Cassandra Tuple data type. You know what you are doing, right?
     *
     * @param a
     * @param b
     * @return a Tuple data type with a and b as the tuples
     */
    public TupleType createTupleType(DataType a, DataType b);

    /**
     * Builder for a session with auto create for keyspace and column family features.
     *
     * @author Sven Wesley
     *
     */
    public static final class SessionBuilder {

        private final String nodes;
        private String keyspace;
        private String replication;
        private String username;
        private String password;
        private Optional<String> datacenter = Optional.empty();
        private List<? extends ColumnDefinition> definitions = Collections.emptyList();
        private Optional<String> preUsername = Optional.empty();
        private Optional<String> prePassword = Optional.empty();

        public final class PreprocessUser {
            private PreprocessUser() {
            }

            public PreprocessPassword asPreprocessUserName(final String prepareUserName) {
                preUsername = Optional.of(prepareUserName);
                return new PreprocessPassword();
            }
        }

        public final class PreprocessPassword {
            private PreprocessPassword() {
            }

            public PreprocessBuild andPreprocessPassword(final String preparePassword) {
                prePassword = Optional.of(preparePassword);
                return new PreprocessBuild();
            }
        }

        public final class PreprocessBuild {
            private PreprocessBuild() {
            }

            public CassandraSession build() {
                return new Build().build();
            }
        }

        public final class Credentials {
            private Credentials() {
            }

            public Credentials asUser(final String sessionPassword) {
                password = Optional.of(sessionPassword).get();
                return this;
            }

            public Prepare andPassword(final String sessionUserName) {
                username = Optional.of(sessionUserName).get();
                return new Prepare();
            }
        }

        public final class Prepare {
            private Prepare() {
            }

            public CassandraSession build() {
                return new Build().build();
            }

            public PreprocessUser preprocessTheseStatements(
                    final List<? extends ColumnDefinition> columnDefinitions) {
                definitions = Optional.of(columnDefinitions).get();
                return new PreprocessUser();
            }
        }

        public final class Build {
            private Build() {
            }

            public CassandraSession build() {
                return new CassandraSessionImpl(username, password, keyspace, nodes, replication, datacenter,
                        preUsername,
                        prePassword, definitions);
            }
        }

        public final class Replication {
            private Replication() {
            }

            public Credentials andReplication(final String replicationStrategy) {
                replication = Optional.of(replicationStrategy).get();
                return new Credentials();
            }
        }

        public SessionBuilder(final String connectNodes) {
            nodes = Optional.of(connectNodes).get();
        }

        public SessionBuilder forDataCenter(final String dataCenter) {
            datacenter = Optional.ofNullable(dataCenter).filter(s -> !s.isEmpty());
            return this;
        }

        public Replication usingKeyspace(final String keyspaceName) {
            keyspace = Optional.of(keyspaceName).get();
            return new Replication();
        }

    }
}
