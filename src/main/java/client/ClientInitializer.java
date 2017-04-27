package client;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import io.netty.handler.ssl.SslContext;

/**
 * Created by carlosmorais on 14/04/2017.
 */
public class ClientInitializer extends ChannelInitializer<SocketChannel> {
    private final SslContext sslCtx;
    private ClientHandler handler;

    public ClientInitializer(SslContext sslCtx, ClientHandler handler) {
        this.sslCtx = sslCtx;
        this.handler = handler;

    }


    @Override
    public void initChannel(SocketChannel ch) {
        ChannelPipeline pipeline = ch.pipeline();

        if (sslCtx != null) {
            pipeline.addLast(sslCtx.newHandler(ch.alloc(), Client.HOST, Client.PORT));
        }

        // and then business logic.
        pipeline.addLast(new ObjectEncoder(),
                new ObjectDecoder(ClassResolvers.cacheDisabled(null)),
                handler);
    }
}
