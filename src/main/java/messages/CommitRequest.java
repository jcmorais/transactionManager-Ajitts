package messages;

import java.io.Serializable;
import java.util.UUID;

/**
 * Created by carlosmorais on 19/04/2017.
 */
public class CommitRequest implements Serializable, MessageEvent {
    private long id; //commitTS
    private  String eventId;




    public CommitRequest(long id) {
        this.id = id;
        this.eventId = UUID.randomUUID().toString();
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }


    @Override
    public String getEventId() {
        return eventId;
    }

    @Override
    public String toString() {
        return "CommitRequest{" +
                "id=" + id +
                ", eventId='" + eventId + '\'' +
                '}';
    }
}
