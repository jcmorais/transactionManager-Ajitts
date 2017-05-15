package hbase;

import client.TmClient;
import messages.BeginReply;

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

    public void commit(Transaction t) throws RollbackException {
        HBaseTransaction tx = (HBaseTransaction) t;
        if(tmClient.commit(t.getTransactionId(), tx.getWriteSet()))
            return;
        else{
            rollback(t);
            throw new RollbackException("conflicts detected in transaction writeset");
        }
    }


    public final void rollback(Transaction transaction)  {

        HBaseTransaction tx = (HBaseTransaction) transaction;

        tx.setStatus(Transaction.Status.ROLLEDBACK);

        //rollback the transaction
        tx.cleanup();

    }
}
