package server;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by carlosmorais on 24/05/2017.
 */
public class AbortedTransactions {

    //private Set<Long> abortedTransactions; // Transactions that were aborted, but the rollback has not yet been confirmed

    private ConcurrentSkipListSet<Long> abortedTransactions;

    // TODO: 24/05/2017 use red/write lock
    private Lock lock;


    public AbortedTransactions() {
        this.abortedTransactions = new ConcurrentSkipListSet<>();
        this.lock = new ReentrantLock();
    }

    public Set<Long> getAbortedTransactions() {
        Set<Long> res = new HashSet<>();

        for (Long abortedTransaction : abortedTransactions)
            res.add(abortedTransaction);

        return res;
    }

    public void addAbortedTransaction(long id) {
        this.abortedTransactions.add(id);
    }

    public void setAbortedTransaction(long id) {
        this.abortedTransactions.remove(id);

    }
}
