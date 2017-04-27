package client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import messages.BeginReply;

import javax.net.ssl.SSLException;
import java.util.Random;

/**
 * Created by carlosmorais on 14/04/2017.
 */
public class Client implements Runnable{

    static final boolean SSL = System.getProperty("ssl") != null;
    static final String HOST = System.getProperty("host", "127.0.0.1");
    static final int PORT = Integer.parseInt(System.getProperty("port", "8322"));



    public void run() {
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
            ClientHandler handler = new ClientHandler();
            b.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ClientInitializer(sslCtx, handler));

            // Make a new connection.
            ChannelFuture f = b.connect(HOST, PORT).sync();

            // Get the handler instance to retrieve the answer.


            // Print out the answer.
            for (int i = 0; i < 200; i++) {
                BeginReply reply = handler.sendRequest();
                System.out.println(reply);
                Random r = new Random();
                //Simulate the client work
                Thread.sleep(r.nextInt(200));
                System.out.println(handler.sendCommitRequest(reply.getCommitTimestamp()));
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            group.shutdownGracefully();
        }
    }
}
