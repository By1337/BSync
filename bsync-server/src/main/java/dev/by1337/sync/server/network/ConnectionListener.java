package dev.by1337.sync.server.network;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import dev.by1337.sync.common.netty.handler.FrameDecoder;
import dev.by1337.sync.common.netty.handler.FrameEncoder;
import dev.by1337.sync.server.DedicatedServer;
import dev.by1337.sync.server.util.LazyLoad;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.ReadTimeoutHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

public class ConnectionListener {
    private static final Logger log = LoggerFactory.getLogger(ConnectionListener.class);
    private final List<ChannelFuture> channels = Collections.synchronizedList(Lists.newArrayList());
    private final LazyLoad<EpollEventLoopGroup> epollEventLoopGroup = new LazyLoad<>(() ->
            new EpollEventLoopGroup(0, new ThreadFactoryBuilder().setNameFormat("Netty Epoll Server IO #%d").setDaemon(true).build())
    );

    private final LazyLoad<NioEventLoopGroup> nioEventLoopGroup = new LazyLoad<>(() ->
            new NioEventLoopGroup(0, new ThreadFactoryBuilder()
                    .setNameFormat("Netty Server IO #%d")
                    .setUncaughtExceptionHandler((t, e) -> log.error("Caught previously unhandled exception :", e)).setDaemon(true).build())
    );

    private final DedicatedServer server;

    public ConnectionListener(DedicatedServer server) {
        this.server = server;
    }


    public void startTcpServerListener(int port) {
        synchronized (channels) {
            Class<? extends ServerSocketChannel> channelClass;
            EventLoopGroup loopGroup;
            if (Epoll.isAvailable()) {
                channelClass = EpollServerSocketChannel.class;
                loopGroup = epollEventLoopGroup.get();
                log.info("Using epoll channel type");
            } else {
                channelClass = NioServerSocketChannel.class;
                loopGroup = nioEventLoopGroup.get();
                log.info("Using default channel type");
            }

            this.channels.add(new ServerBootstrap().channel(channelClass)
                    .childHandler(new ChannelInitializer<>() {
                        protected void initChannel(final Channel channel) {
                            try {
                                channel.config().setOption(ChannelOption.TCP_NODELAY, true);
                            } catch (ChannelException ignored) {
                            }

                            channel.pipeline()
                                    //  .addLast(new FlushConsolidationHandler())
                                    .addLast("timeout", new ReadTimeoutHandler(30))
                                    .addLast("splitter", new FrameDecoder())
                                    .addLast("prepender", new FrameEncoder())
                                    .addLast("login", new LoginPacketListener(server))
                            ;
                        }
                    })
                    .group(loopGroup)
                    .localAddress(port)
                    .bind()
                    .syncUninterruptibly());

        }
    }

    public void stop() {
        synchronized (this.channels) {
            for (ChannelFuture channelFuture : this.channels) {
                try {
                    channelFuture.channel().close().sync();
                } catch (InterruptedException var4) {
                    log.error("Interrupted whilst closing channel");
                }
            }
            this.channels.clear();
        }

    }
}
