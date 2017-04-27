package server;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import io.netty.handler.ssl.SslContext;
import test.Run;

/**
 * Created by carlosmorais on 14/04/2017.
 */
public class ServerInitializer extends ChannelInitializer<SocketChannel> {
    private final SslContext sslCtx;
    Sheduler sheduller;

    public ServerInitializer(SslContext sslCtx, Timestamp timestamp) {
        this.sslCtx = sslCtx;
        this.sheduller = new Sheduler();
         Runnable task = () ->{
            sheduller.run();
        };
        Thread thread = new Thread(task);
        thread.start();

    }


    @Override
    public void initChannel(SocketChannel ch) {
        ChannelPipeline pipeline = ch.pipeline();

        if (sslCtx != null) {
            pipeline.addLast(sslCtx.newHandler(ch.alloc()));
        }




        // Please note we create a handler for every new channel
        // because it has stateful properties.
        pipeline.addLast(new ObjectEncoder(),
                new ObjectDecoder(ClassResolvers.cacheDisabled(null)),
                new ServerHandler(sheduller));
    }
}
