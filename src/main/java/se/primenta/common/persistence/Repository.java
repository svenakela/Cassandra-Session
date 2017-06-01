package se.primenta.common.persistence;

/**
 * Save to the storage layer interface.
 *
 * @author Sven Wesley
 *
 * @param <T>
 *            The entity type handled by this repository.
 */
public interface Repository<T> {

    /**
     * Persist on the entity.
     *
     * @param entity
     *            The object to write to storage layer by the implementing persister.
     */
    void persist(T entity);

}
