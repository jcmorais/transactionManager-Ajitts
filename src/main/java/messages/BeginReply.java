package messages;

import java.io.Serializable;
import java.util.Set;
import java.util.UUID;

/**
 * Created by carlosmorais on 14/04/2017.
 */
public class BeginReply implements Serializable, MessageEvent{
    private long startTimestamp;
    private long commitTimestamp;
    private String eventId;

    private Set<Long> abortedTransactions;


    public BeginReply(long startTimestamp, long commitTimestamp, String eventId, Set<Long> abortedTransactions) {
        this.startTimestamp = startTimestamp;
        this.commitTimestamp = commitTimestamp;
        this.eventId = eventId;
        this.abortedTransactions = abortedTransactions;
    }

    public BeginReply() {
        this.startTimestamp = 0;
        this.commitTimestamp = 0;
    }

    public Set<Long> getAbortedTransactions() {
        return abortedTransactions;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public void setStartTimestamp(long startTimestamp) {
        this.startTimestamp = startTimestamp;
    }

    public long getCommitTimestamp() {
        return commitTimestamp;
    }

    public void setCommitTimestamp(long commitTimestamp) {
        this.commitTimestamp = commitTimestamp;
    }

    @Override
    public String getEventId() {
        return eventId;
    }

    @Override
    public String toString() {
        return "BeginReply{" +
                "startTimestamp=" + startTimestamp +
                ", commitTimestamp=" + commitTimestamp +
                ", eventId='" + eventId + '\'' +
                ", abortedTransactions=" + abortedTransactions +
                '}';
    }

}
