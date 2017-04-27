package client;

/**
 * Created by carlosmorais on 12/04/2017.
 */
public interface TransactionManagerAjitts {

    TransactionAjitts begin();

    boolean commit(TransactionAjitts t);
}
