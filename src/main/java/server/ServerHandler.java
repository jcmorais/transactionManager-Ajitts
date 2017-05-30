package server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import messages.*;

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
            sheduler.commitTransaction((CommitRequest) msg);
        }
        else if (msg instanceof RollbackDone){
            //sheduler.abortedTransactions.removeTransaction(((RollbackDone) msg).getId());
            sheduler.timestamp.updateStartTS(((RollbackDone) msg).getId());
        }
    }
}
