package hbase;

/**
 * Created by carlosmorais on 10/05/2017.
 */
public interface TransactionManager {
    Transaction begin() throws TransactionException;
    void commit(Transaction tx) throws RollbackException, TransactionException;
}
