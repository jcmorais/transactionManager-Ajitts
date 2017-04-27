package messages;

import java.io.Serializable;
import java.util.UUID;

/**
 * Created by carlosmorais on 22/04/2017.
 */
public class CommitReply implements Serializable, MessageEvent {
    private boolean commit;
    private String eventId;

    public CommitReply(boolean commit, String eventId) {
        this.commit = commit;
        this.eventId = eventId;
    }

    public boolean isCommit() {
        return commit;
    }

    public void setCommit(boolean commit) {
        this.commit = commit;
    }

    @Override
    public String toString() {
        return "CommitReply{" +
                "commit=" + commit +
                ", eventId='" + eventId + '\'' +
                '}';
    }

    @Override
    public String getEventId() {
        return eventId;
    }

}
