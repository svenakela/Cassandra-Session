package se.primenta.common.persistence;

/**
 * Specific runtime exception for persistence problems.
 *
 * @author Sven Wesley
 *
 */
public class PersistenceRuntimeException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public PersistenceRuntimeException(final String message) {
        super(message);
    }

    public PersistenceRuntimeException(final Throwable t) {
        super(t);
    }

    public PersistenceRuntimeException(final String message, final Throwable t) {
        super(message, t);
    }

}