package server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by carlosmorais on 24/05/2017.
 */
public class AbortedTransactions {
    private static final Logger LOG = LoggerFactory.getLogger(AbortedTransactions.class);
    //private Set<Long> abortedTransactions; // Transactions that were aborted, but the rollback has not yet been confirmed

    private ConcurrentSkipListSet<Long> abortedTransactions;


    // TODO: 24/05/2017 use red/write lock
    private Lock lock;


    public AbortedTransactions() {
        this.abortedTransactions = new ConcurrentSkipListSet<>();
        this.lock = new ReentrantLock();
    }

    public Set<Long> getAbortedTransactions() {
        return abortedTransactions;
    }

    public void addAbortedTransaction(long id) {
        this.abortedTransactions.add(id);
    }

    public void removeTransaction(long id) {
        this.abortedTransactions.remove(id);
    }
}
