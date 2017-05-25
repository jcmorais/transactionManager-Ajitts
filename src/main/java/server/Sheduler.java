package server;

import io.netty.channel.Channel;
import messages.BeginReply;
import messages.BeginRequest;
import messages.CommitReply;
import messages.CommitRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by carlosmorais on 20/04/2017.
 */
public class Sheduler implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(Sheduler.class);

    BlockingDeque<Transaction> queue;
    private Timestamp timestamp;

    Map<Long, Transaction> transactionMap;

    AbortedTransactions abortedTransactions;

    private final CommitHashMap hashmap;


    public Sheduler() {
        this.queue = new LinkedBlockingDeque<>();
        this.timestamp = new TimestampImpl();
        this.transactionMap = new ConcurrentHashMap<>();
        this.hashmap = new CommitHashMap(10000000);
        this.abortedTransactions = new AbortedTransactions();
    }




    public void startTransaction(BeginRequest event, Channel channel){
        LOG.debug("event={} request to start a new transaction", event.getEventId());
        Transaction t = new Transaction(this, channel);
        synchronized (queue){
            t.setCommitTS(timestamp.nextCommitTS());
            queue.offer(t);
            transactionMap.put(t.getCommitTS(), t);
        }

        /*
        try {
            Random r = new Random();
            int s = r.nextInt(125);
            Thread.sleep(s);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        */
        t.setStartTS(timestamp.getStartTS());
        BeginReply reply = new BeginReply(t.getStartTS(), t.getCommitTS(), event.getEventId(), abortedTransactions.getAbortedTransactions());
        try {
            channel.writeAndFlush(reply).sync();
        } catch (InterruptedException e) {
            System.out.println("FODEU "+t.getCommitTS());
        }

        LOG.debug("event={} start a new transaction: startTS={} commitTS={}", event.getEventId(), t.getStartTS(), t.getCommitTS());
    }

    public void commitTransaction(CommitRequest event){
        //Mark the transaction as ready to commit
        //Scheduler is responsible for responding to the client with the result
        transactionMap.get(event.getId()).setCommitRequest(event.getEventId(), event.getCellId());
    }


    private boolean checkConflicts(Transaction tx) {
        LOG.debug("check conflict in transaction {}",tx.toString());
        boolean txCanCommit=true;
        long startTimestamp = tx.getStartTS();
        Iterable<Long> writeSet = tx.getCellsId();

        int numCellsInWriteset = 0;
        // 1. check the write-write conflicts
        for (long cellId : writeSet) {
            long value = hashmap.getLatestWriteForCell(cellId);
            if (value != 0 && value > startTimestamp) {
                txCanCommit = false;
                break;
            }
            numCellsInWriteset++;
        }

        if (txCanCommit) {
            // 2. commit
            long commitTS = tx.getCommitTS();
            if (numCellsInWriteset > 0) {
                for (long r : writeSet)
                    hashmap.putLatestWriteForCell(r, commitTS);
            }
        }
        LOG.debug("transaction={} commit={}",tx.getCommitTS(), txCanCommit);
        return txCanCommit;
    }


    @Override
    public void run() {

        Transaction nextTx;
        while (true){
            try {
                nextTx = queue.take();
                LOG.debug("Take (st={},cm={})",nextTx.getStartTS(), nextTx.getCommitTS());
                nextTx.waitForCommitRequest();

                //Deteção de conflitos
                boolean commit = checkConflicts(nextTx);
                timestamp.updateStartTS(nextTx.getCommitTS());

                if (!commit)
                    abortedTransactions.addAbortedTransaction(nextTx.getCommitTS());

                //reply to the Client
                CommitReply reply = new CommitReply(commit, nextTx.getEventId());

                nextTx.getChannel().writeAndFlush(reply);
                LOG.debug("Done {}", nextTx.getCommitTS());

                transactionMap.remove(nextTx.getCommitTS());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


    }


}
