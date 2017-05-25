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

    private ConcurrentSkipListSet<Long> abortedTransactions; // Transactions that were aborted, but the rollback has not yet been confirmed


    public AbortedTransactions() {
        this.abortedTransactions = new ConcurrentSkipListSet<>();
    }

    public Set<Long> getAbortedTransactions() {
        return abortedTransactions.clone();
    }

    public void addAbortedTransaction(long id) {
        this.abortedTransactions.add(id);
    }

    public void removeTransaction(long id) {
        this.abortedTransactions.remove(id);
    }
}
