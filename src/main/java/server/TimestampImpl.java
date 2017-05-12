package server;

/**
 * Created by carlosmorais on 12/04/2017.
 */
public class  TimestampImpl implements Timestamp{

    private long startTimestamp;
    private long commitTimestamp;


    public TimestampImpl() {
        this.startTimestamp = 0;
        this.commitTimestamp = 0;
    }


    public synchronized long nextCommitTS() {
        commitTimestamp++;
        return commitTimestamp;
    }

    public synchronized long nextStartTS() {
        startTimestamp++;
        return startTimestamp;
    }

    public synchronized void updateStartTS(long commitTS){
        startTimestamp = commitTS;
    }

    public long getStartTS() {
        return startTimestamp;
    }

    public long getCommitTS() {
        return commitTimestamp;
    }

    @Override
    public String toString() {
        return "TimestampImpl{" +
                "startTimestamp=" + startTimestamp +
                ", commitTimestamp=" + commitTimestamp +
                '}';
    }
}
