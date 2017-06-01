package client;

import hbase.CellId;
import hbase.HBaseTransactionManager;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * Created by carlosmorais on 14/04/2017.
 */
public class ClientHandler extends SimpleChannelInboundHandler<MessageEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(ClientHandler.class);
    private ChannelHandlerContext ctx;
    final BlockingQueue<MessageEvent> answer = new LinkedBlockingQueue<MessageEvent>();
    ConcurrentHashMap<String, BlockingQueue<MessageEvent>> promises = new ConcurrentHashMap<>();

    //ConcurrentHashMap<String,Promise<MessageEvent>> responses = new ConcurrentHashMap<>();
    ConcurrentHashMap<String,ResponseFuture> responses = new ConcurrentHashMap<>();

    protected void channelRead0(ChannelHandlerContext channelHandlerContext, MessageEvent messageEvent) throws Exception {

        responses.get(messageEvent.getEventId()).set(messageEvent);

        /*
        System.out.println(promises);
        System.out.println(messageEvent);
        promises.get(messageEvent.getEventId()).offer(messageEvent);
        */
        //answer.offer(messageEvent);
    }



    public MessageEvent sendEvent(MessageEvent e){

        if (e instanceof RollbackDone) {
            ctx.writeAndFlush(e);
            return null;
        }
        else if (e instanceof WritesDone) {
            ctx.writeAndFlush(e);
            return null;
        }

        final ResponseFuture responseFuture = new ResponseFuture();
        responses.put(e.getEventId(), responseFuture);

        ctx.writeAndFlush(e);

        try {
            return responseFuture.get();
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        } catch (ExecutionException e1) {
            e1.printStackTrace();
        }

        return  null;
    }


    public BeginReply sendRequest(){
        BeginRequest req = new BeginRequest();
        return (BeginReply) sendEvent(req);
        /*
        BeginRequest req = new BeginRequest();
        try {
            promises.put(req.getEventId(), new LinkedBlockingQueue<>());
            ctx.writeAndFlush(req);
            return (BeginReply) promises.get(req.getEventId()).take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;*/
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }

    public CommitReply sendCommitRequest(long commitTimestamp, Set<? extends CellId> cells) {

        CommitRequest req = new CommitRequest(commitTimestamp, cells);
        return (CommitReply) sendEvent(req);

        /*
        try {
            promises.put(req.getEventId(), new LinkedBlockingQueue<>());
            ctx.writeAndFlush(req);
            return (CommitReply) promises.get(req.getEventId()).take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
        */

    }
}
