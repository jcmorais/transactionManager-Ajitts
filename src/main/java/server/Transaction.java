package server;

import hbase.AbstractTransaction;
import io.netty.channel.Channel;

import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by carlosmorais on 19/04/2017.
 */
public class Transaction implements Runnable{
    private long commitTS;
    private long startTS;
    private boolean commitRequest;
    ReentrantLock lock;
    private Condition condition;

    private Sheduler sheduler;
    private Channel channel;

    private String eventId;


    private List<Long> cellsId;

    private Status status;


    enum Status {
        RUNNING, COMMITTED, ROLLEDBACK
    }

    public Transaction(Sheduler sheduler, Channel channel) {
        this.commitRequest = false;
        this.sheduler = sheduler;
        this.channel = channel;
        this.lock = new ReentrantLock();
        this.condition = lock.newCondition();
        this.status = Status.RUNNING;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }


    public List<Long> getCellsId() {
        return cellsId;
    }

    public void setCellsId(List<Long> cellsId) {
        this.cellsId = cellsId;
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


    public void setCommitRequest(String eventId, List<Long> cells) {
        lock.lock();
        try {
            commitRequest = true;
            this.eventId = eventId;
            this.cellsId = cells;
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


    public long getStartTS() {
        return startTS;
    }

    public void setStartTS(long startTS) {
        this.startTS = startTS;
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
        return "Transaction{" +
                "commitTS=" + commitTS +
                ", startTS=" + startTS +
                ", commitRequest=" + commitRequest +
                ", lock=" + lock +
                ", condition=" + condition +
                ", sheduler=" + sheduler +
                ", channel=" + channel +
                ", eventId='" + eventId + '\'' +
                ", cellsId=" + cellsId +
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
