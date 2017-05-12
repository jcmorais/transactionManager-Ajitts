package client;

import hbase.CellId;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import messages.BeginReply;
import messages.MessageEvent;

import javax.net.ssl.SSLException;
import java.util.Random;
import java.util.Set;

/**
 * Created by carlosmorais on 24/04/2017.
 */
public class TmClient {

    static final boolean SSL = System.getProperty("ssl") != null;
    static final String HOST = System.getProperty("host", "192.168.112.57");
    static final int PORT = Integer.parseInt(System.getProperty("port", "8322"));

    private ClientHandler handler = new ClientHandler();


    public TmClient() {


        // Configure SSL.
        SslContext sslCtx = null;
        if (SSL) {
            try {
                sslCtx = SslContextBuilder.forClient()
                        .trustManager(InsecureTrustManagerFactory.INSTANCE).build();
            } catch (SSLException e) {
                e.printStackTrace();
            }
        }

        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ClientInitializer(sslCtx, handler));

            // Make a new connection.
            ChannelFuture f = b.connect(HOST, PORT).sync();
            f.isSuccess();


        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            //group.shutdownGracefully();
        }
    }



    public BeginReply begin(){
        return handler.sendRequest();
    }

    public boolean commit(long id, Set<? extends CellId> cells){
        return handler.sendCommitRequest(id, cells).isCommit();
    }
}
