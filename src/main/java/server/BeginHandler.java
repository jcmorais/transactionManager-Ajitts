package server;

import messages.BeginRequest;

/**
 * Created by carlosmorais on 22/04/2017.
 */
public class BeginHandler implements Runnable{

    BeginRequest beginRequest;
    Sheduler sheduler;

    public BeginHandler(BeginRequest beginRequest, Sheduler sheduler) {
        this.beginRequest = beginRequest;
        this.sheduler = sheduler;
    }

    @Override
    public void run() {

    }
}
