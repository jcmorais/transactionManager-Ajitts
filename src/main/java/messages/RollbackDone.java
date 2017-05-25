package messages;

import java.io.Serializable;
import java.util.UUID;

/**
 * Created by carlosmorais on 25/05/2017.
 */
public class RollbackDone implements Serializable, MessageEvent {
    private String eventId;
    private Long id; //txId

    public RollbackDone(long id) {
        this.eventId = UUID.randomUUID().toString();
        this.id = id;
    }

    @Override
    public String getEventId() {
        return eventId;
    }

    public Long getId() {
        return id;
    }

    @Override
    public String toString() {
        return "RollbackDone{" +
                "eventId='" + eventId + '\'' +
                ", id=" + id +
                '}';
    }
}
