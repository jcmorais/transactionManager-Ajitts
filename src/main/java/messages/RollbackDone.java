package messages;

import java.io.Serializable;
import java.util.UUID;

/**
 * Created by carlosmorais on 22/05/2017.
 */
public class RollbackDone implements MessageEvent, Serializable {
    private String eventId;
    private long transactionId;

    public RollbackDone() {
        this.eventId = UUID.randomUUID().toString();
    }

    public long getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(long transactionId) {
        this.transactionId = transactionId;
    }

    @Override
    public String getEventId() {
        return eventId;
    }

    @Override
    public String toString() {
        return "RollbackDone{" +
                "eventId='" + eventId + '\'' +
                ", transactionId=" + transactionId +
                '}';
    }
}
