package server;

import io.netty.channel.Channel;
import messages.BeginReply;
import messages.BeginRequest;
import messages.CommitReply;
import messages.CommitRequest;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by carlosmorais on 20/04/2017.
 */
public class Sheduler implements Runnable {

    BlockingDeque<Transaction> queue;
    private Timestamp timestamp;

    Map<Long, Transaction> transactionMap;


    public Sheduler() {
        this.queue = new LinkedBlockingDeque<>();
        this.timestamp = new TimestampImpl();
        this.transactionMap = new ConcurrentHashMap<>();
    }


    public void startTransaction(BeginRequest event, Channel channel){
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
        t.setBeginTS(timestamp.nextBeginTS());
        BeginReply reply = new BeginReply(t.getBeginTS(), t.getCommitTS(), event.getEventId());
        channel.writeAndFlush(reply);
    }

    public void commitTransaction(CommitRequest event){
        //Mark the transaction as ready to commit
        //Scheduler is responsible for responding to the client with the result
        transactionMap.get(event.getId()).setCommitRequest(event.getEventId());
    }




    @Override
    public void run() {

        Transaction nextTx;
        while (true){
            try {
                nextTx = queue.take();
                System.out.println("Take: "+nextTx);
                nextTx.waitForCommitRequest();
                //Deteção de conflitos

                //reply to the Client
                CommitReply reply = new CommitReply(true, nextTx.getEventId());

                nextTx.getChannel().writeAndFlush(reply);
                System.out.println("Done "+nextTx.getCommitTS());

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


    }
}
