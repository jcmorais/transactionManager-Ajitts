package server;

/**
 * Created by carlosmorais on 12/04/2017.
 */
public class  TimestampImpl implements Timestamp{

    private long lastTimestamp;

    private long beginTimestamp;
    private long commitTimestamp;



    public TimestampImpl() {
        this.lastTimestamp = 0;
        this.beginTimestamp = 0;
        this.commitTimestamp = 0;
    }



    public synchronized long nextCommitTS() {
        commitTimestamp++;
        return commitTimestamp;
    }

    public synchronized long nextBeginTS() {
        beginTimestamp++;
        return beginTimestamp;
    }


    public long getBeginTimestamp() {
        return beginTimestamp;
    }

    public long getCommitTimestamp() {
        return commitTimestamp;
    }

    @Deprecated
    public synchronized long next() {
        lastTimestamp++;
        return  lastTimestamp;
    }

    @Override
    public String toString() {
        return "TimestampImpl{" +
                "lastTimestamp=" + lastTimestamp +
                ", beginTimestamp=" + beginTimestamp +
                ", commitTimestamp=" + commitTimestamp +
                '}';
    }
}
