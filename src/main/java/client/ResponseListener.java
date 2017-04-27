package client;

import messages.MessageEvent;

/**
 * Created by carlosmorais on 27/04/2017.
 */
public class ResponseListener {
    private MessageEvent event;


    public MessageEvent getEvent() {
        return event;
    }

    public void setEvent(MessageEvent event) {
        this.event = event;
    }
}
