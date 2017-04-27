package server;

import io.netty.channel.Channel;
import messages.BeginReply;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by carlosmorais on 19/04/2017.
 */
public class Transaction implements Runnable{
    private long commitTS;
    private long beginTS;
    private boolean commitRequest;
    ReentrantLock lock;
    private Condition condition;

    private Sheduler sheduler;
    private Channel channel;

    private String eventId;



    public Transaction(Sheduler sheduler, Channel channel) {
        this.commitRequest = false;
        this.sheduler = sheduler;
        this.channel = channel;
        this.lock = new ReentrantLock();
        this.condition = lock.newCondition();
    }


    public void waitForCommitRequest(){
        lock.lock();
        try {
            while (!commitRequest) {
                try {
                    condition.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        finally {
            lock.unlock();
        }
    }


    public void setCommitRequest(String eventId) {
        lock.lock();
        try {
            commitRequest = true;
            this.eventId = eventId;
            condition.signalAll();
        }
        finally {
            lock.unlock();
        }
    }

    public long getCommitTS() {
        return commitTS;
    }

    public void setCommitTS(long commitTS) {
        this.commitTS = commitTS;
    }


    public long getBeginTS() {
        return beginTS;
    }

    public void setBeginTS(long beginTS) {
        this.beginTS = beginTS;
    }


    public Channel getChannel() {
        return channel;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Transaction that = (Transaction) o;

        return commitTS == that.commitTS;
    }

    @Override
    public int hashCode() {
        return (int) (commitTS ^ (commitTS >>> 32));
    }

    @Override
    public String toString() {
        return "TransactionAjitts{" +
                "commitTS=" + commitTS +
                ", beginTS=" + beginTS +
                ", commitRequest=" + commitRequest +
                ", lock=" + lock +
                ", sheduler=" + sheduler +
                ", channel=" + channel +
                '}';
    }

    @Override
    public void run() {
        /*
        sheduler.startTransaction(this);
        //posso fazer aqui um sleep para simular o escalonamento
        sheduler.beginTransaction(this);
        channel.writeAndFlush(new BeginReply());
        */
        //wait for work done
    }

    public String getEventId() {
        return eventId;
    }
}
