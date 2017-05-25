package server;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by carlosmorais on 24/05/2017.
 */
public class AbortedTransactions {

    private Set<Long> abortedTransactions; // Transactions that were aborted, but the rollback has not yet been confirmed


    // TODO: 24/05/2017 use red/write lock
    private Lock lock;


    public AbortedTransactions() {
        this.abortedTransactions = new HashSet<>();
        this.lock = new ReentrantLock();
    }

    public Set<Long> getAbortedTransactions() {
        Set<Long> res = new HashSet<>();
        try {
            lock.lock();
            for (Long abortedTransaction : abortedTransactions) {
                res.add(abortedTransaction);
            }
        }
        finally {
            lock.unlock();
        }
        return res;
    }

    public void addAbortedTransaction(long id) {
        try {
            lock.lock();
            this.abortedTransactions.add(id);
        }
        finally {
            lock.unlock();
        }
    }

    public void setAbortedTransaction(long id) {
        try {
            lock.lock();
            this.abortedTransactions.remove(id);
        }
        finally {
            lock.unlock();
        }
    }
}
