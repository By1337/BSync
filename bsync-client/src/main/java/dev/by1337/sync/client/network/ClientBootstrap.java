package dev.by1337.sync.client.network;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import dev.by1337.sync.common.netty.handler.FrameDecoder;
import dev.by1337.sync.common.netty.handler.FrameEncoder;
import dev.by1337.sync.common.netty.handler.PacketDecoder;
import dev.by1337.sync.common.netty.handler.PacketEncoder;
import dev.by1337.sync.common.packet.Packets;
import dev.by1337.sync.common.util.LazyLoad;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ClientBootstrap {
    private static final int EVENT_LOOP_THREADS = Integer.getInteger("bcsync.client.netty.threads", 1);
    private static final Logger log = LoggerFactory.getLogger(ClientBootstrap.class);
    private static final LazyLoad<EpollEventLoopGroup> epollEventLoopGroup = new LazyLoad<>(() ->
            new EpollEventLoopGroup(EVENT_LOOP_THREADS, new ThreadFactoryBuilder().setNameFormat("Netty Epoll BSync IO #%d").setDaemon(true).build())
    );

    private static final LazyLoad<NioEventLoopGroup> nioEventLoopGroup = new LazyLoad<>(() ->
            new NioEventLoopGroup(EVENT_LOOP_THREADS, new ThreadFactoryBuilder()
                    .setNameFormat("Netty BSync IO #%d")
                    .setUncaughtExceptionHandler((t, e) -> log.error("Caught previously unhandled exception :", e)).setDaemon(true).build())
    );
    private final Bootstrap bootstrap;

    public ClientBootstrap() {
        Class<? extends SocketChannel> channelClass;
        EventLoopGroup loopGroup;
        if (Epoll.isAvailable()) {
            channelClass = EpollSocketChannel.class;
            loopGroup = epollEventLoopGroup.get();
        } else {
            channelClass = NioSocketChannel.class;
            loopGroup = nioEventLoopGroup.get();
        }
        bootstrap = new Bootstrap()
                .group(loopGroup)
                .channel(channelClass)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<>() {
                    @Override
                    protected void initChannel(Channel ch) {
                        ch.pipeline()
                                .addLast("splitter", new FrameDecoder())
                                .addLast("decoder", new PacketDecoder(Packets.PROTOCOL_VERSION))
                                .addLast("prepender", new FrameEncoder())
                                .addLast("encoder", new PacketEncoder(Packets.PROTOCOL_VERSION))
                                ;
                    }
                });
    }

    public ChannelFuture connect(String host, int port) {
        return bootstrap.connect(host, port);
    }
}
