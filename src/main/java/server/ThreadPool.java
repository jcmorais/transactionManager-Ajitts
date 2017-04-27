package server;

import io.netty.channel.Channel;
import messages.BeginRequest;
import messages.MessageEvent;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Created by carlosmorais on 22/04/2017.
 */
public class ThreadPool{

    private ExecutorService beginService;
    private ExecutorService commitService;

    public ThreadPool() {
        this.beginService = new ScheduledThreadPoolExecutor(25);
        this.commitService = new ScheduledThreadPoolExecutor(25);
    }


    public synchronized void addEvent(MessageEvent event, Channel channel){
        if (event instanceof BeginRequest)
            beginService.submit(new Transaction(null, channel));
    }
}
