package dev.by1337.sync.server.network;

import dev.by1337.sync.common.channel.pipeline.SocketConnection;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.impl.PingPacket;
import dev.by1337.sync.common.packet.impl.PongPacket;
import dev.by1337.sync.server.DedicatedServer;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class Connection extends SimpleChannelInboundHandler<Packet> implements SocketConnection {
    private static final Logger log = LoggerFactory.getLogger(Connection.class);
    private final Channel channel;
    private final DedicatedServer server;
    private final String id;
    private final int protocolVersion;
    private final AtomicBoolean flushScheduled = new AtomicBoolean();
    private int ping;

    public Connection(Channel channel, DedicatedServer server, String id, int protocolVersion) {
        this.channel = channel;
        this.server = server;
        this.id = id;
        this.protocolVersion = protocolVersion;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Packet msg) throws Exception {
        if (msg instanceof PingPacket p) {
            write(new PongPacket(System.currentTimeMillis()));
            write(new PingPacket());
        } else if (msg instanceof PongPacket(long timestamp)) {
            ping = (int) (System.currentTimeMillis() - timestamp);
        } else {
            server.channelManager().onReceive(msg, this);
        }
    }

    public void write(Packet packet) {
        channel.write(packet);
        if (flushScheduled.compareAndSet(false, true)) {
            channel.eventLoop().schedule(() -> {
                channel.flush();
                flushScheduled.set(false);
            }, 2, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        disconnect(ctx, "End of stream");
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        super.channelUnregistered(ctx);
        disconnect(ctx, "connection unregister");
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (cause instanceof TimeoutException) {
            this.disconnect(ctx, "Timed out");
        } else {
            this.disconnect(ctx, "Internal Exception: " + cause);
            log.error("An error occurred in the {} connection", ctx.channel().remoteAddress(), cause);
        }
    }

    private void disconnect(ChannelHandlerContext ctx, String message) {
        if (ctx.channel().isOpen()) {
            ctx.channel().close();
            log.info("Disconnect unauthorized connection {}, reason: {}", ctx.channel().remoteAddress(), message);
        }
        server.onDisconnect(this);
    }

    public Channel channel() {
        return channel;
    }

    public DedicatedServer server() {
        return server;
    }

    public String id() {
        return id;
    }

    public int protocolVersion() {
        return protocolVersion;
    }

    public int ping() {
        return ping;
    }
}
