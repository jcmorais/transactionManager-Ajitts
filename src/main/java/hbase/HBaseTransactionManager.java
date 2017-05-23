package hbase;

import client.TmClient;
import messages.BeginReply;

import java.io.IOException;
import java.util.HashSet;

/**
 * Created by carlosmorais on 12/04/2017.
 */
public class HBaseTransactionManager {

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
                this);
        return t;
    }

    public void commit(Transaction t) throws RollbackException, IOException {
        HBaseTransaction tx = (HBaseTransaction) t;
        tx.flushTables();

        if(tmClient.commit(t.getTransactionId(), tx.getWriteSet()))
            return;
        else{
            rollback(t);
            throw new RollbackException("conflicts detected in transaction writeset");
        }
    }


    public final void rollback(Transaction transaction) throws IOException {

        HBaseTransaction tx = (HBaseTransaction) transaction;
        tx.flushTables();
        tx.setStatus(Transaction.Status.ROLLEDBACK);

        //rollback the transaction
        tx.cleanup();

    }
}
