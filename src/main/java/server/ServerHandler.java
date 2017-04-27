package server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import messages.CommitRequest;
import messages.MessageEvent;
import messages.BeginReply;
import messages.BeginRequest;

import java.util.Random;

/**
 * Created by carlosmorais on 14/04/2017.
 */
public class ServerHandler extends SimpleChannelInboundHandler<MessageEvent> {

    private Sheduler sheduler;

    public ServerHandler(Sheduler sheduler) {
        this.sheduler = sheduler;
    }

    protected void channelRead0(ChannelHandlerContext ctx, MessageEvent msg) throws Exception {

        if( msg instanceof BeginRequest){
            sheduler.startTransaction((BeginRequest) msg, ctx.channel());
        }
        else if( msg instanceof CommitRequest){
            System.out.println(msg);
            sheduler.commitTransaction((CommitRequest) msg);
        }
    }
}
