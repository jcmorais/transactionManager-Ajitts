package messages;

import java.io.Serializable;

/**
 * Created by carlosmorais on 31/05/2017.
 */
public class WritesDone implements Serializable, MessageEvent {
    private Long id;

    public WritesDone(Long id) {
        this.id = id;
    }

    public Long getId() {
        return id;
    }

    @Override
    public String getEventId() {
        return "";
    }
}
