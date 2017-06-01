package server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Created by carlosmorais on 12/04/2017.
 */
public class  TimestampImpl implements Timestamp{
    private static final Logger LOG = LoggerFactory.getLogger(TimestampImpl.class);

    private long startTimestamp;
    private long commitTimestamp;

    private ConcurrentSkipListSet<Long> pendingStarts;


    public TimestampImpl() {
        this.startTimestamp = 0;
        this.commitTimestamp = 0;
        this.pendingStarts = new ConcurrentSkipListSet<>();
    }


    public synchronized void updateStartTS(long commitTS){
        LOG.info("update startTS with {}, current={}", commitTS, startTimestamp);
        if (startTimestamp == (commitTS-1)) {
            startTimestamp = commitTS;
            updatePendings();
        }
        else  {
            LOG.info("add "+commitTS+" to "+pendingStarts);
            pendingStarts.add(commitTS);
            LOG.info("so pendings "+pendingStarts);
        }
    }

    public void updatePendings(){
        LOG.info("update pendings; startTS={}, pendings={}", startTimestamp, pendingStarts);
        Iterator<Long> it = pendingStarts.iterator();
        boolean flag = true;
        while (it.hasNext() && flag){
            long current = it.next();
            if (current == (startTimestamp+1)) {
                startTimestamp = current;
                pendingStarts.remove(current);
            }
            else
                flag=false;
        }
    }

    public synchronized long nextCommitTS() {
        commitTimestamp++;
        return commitTimestamp;
    }

    public synchronized long nextStartTS() {
        startTimestamp++;
        return startTimestamp;
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
