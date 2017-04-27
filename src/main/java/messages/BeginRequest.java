package messages;

import java.io.Serializable;
import java.util.UUID;

/**
 * Created by carlosmorais on 14/04/2017.
 */
public class BeginRequest implements Serializable, MessageEvent{
    private String eventId;

    public BeginRequest() {
        this.eventId = UUID.randomUUID().toString();
    }

    @Override
    public String getEventId() {
        return eventId;
    }

    @Override
    public String toString() {
        return "BeginRequest{" +
                "eventId='" + eventId + '\'' +
                '}';
    }
}
