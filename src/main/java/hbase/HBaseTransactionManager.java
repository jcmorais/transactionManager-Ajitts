package hbase;

import client.TmClient;
import messages.BeginReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import server.Sheduler;

import java.io.IOException;
import java.util.HashSet;

/**
 * Created by carlosmorais on 12/04/2017.
 */
public class HBaseTransactionManager {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseTransactionManager.class);

    private TmClient tmClient;

    public HBaseTransactionManager() {
        this.tmClient = new TmClient();
    }

    public Transaction begin() {
        BeginReply reply = tmClient.begin();

        Transaction t = new HBaseTransaction(
                reply.getStartTimestamp(),
                reply.getCommitTimestamp(),
                new HashSet<>(),
                this,
                reply.getAbortedTransactions());

        LOG.debug("Trasaction={} begin; abortedTx={}", t.getTransactionId(), ((HBaseTransaction )t).getAbortedTransactions());
        return t;
    }

    public void commit(Transaction t) throws RollbackException, IOException {
        LOG.debug("Trasaction={} try to commit", t.getTransactionId());
        HBaseTransaction tx = (HBaseTransaction) t;
        tx.flushTables();
        if(tmClient.commit(t.getTransactionId(), tx.getWriteSet())) {
            LOG.debug("Trasaction={} commit done", t.getTransactionId());
            return;
        }
        else{
            LOG.debug("Trasaction={} abort, need to rollback", t.getTransactionId());
            rollback(t);
            throw new RollbackException("conflicts detected in transaction writeset");
        }
    }


    public final void rollback(Transaction transaction) throws IOException {

        HBaseTransaction tx = (HBaseTransaction) transaction;
        tx.flushTables();
        tx.setStatus(Transaction.Status.ROLLEDBACK);

        //rollback the transaction

        /*
        //use this to test
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        */
        tx.cleanup();
        tmClient.rollbackDone(transaction.getTransactionId());
        LOG.debug("Trasaction={} rollback done", transaction.getTransactionId());

    }
}
