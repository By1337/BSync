package dev.by1337.sync.client.network;

import dev.by1337.sync.client.config.ConnectionConfig;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import dev.by1337.sync.common.packet.impl.c2s.C2SHelloPacket;
import dev.by1337.sync.common.packet.impl.c2s.C2SLoginPacket;
import dev.by1337.sync.common.packet.impl.s2c.S2CNoncePacket;
import dev.by1337.sync.common.packet.impl.s2c.S2CPostLoginPacket;
import dev.by1337.sync.common.security.Ed25519;
import io.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.PrivateKey;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConnectionHandler extends SimpleChannelInboundHandler<Packet> {
    private static final Logger log = LoggerFactory.getLogger(ConnectionHandler.class);

    private final String id;
    public final ConnectionConfig connectionConfig;
    private final ClientBootstrap bootstrap;
    private volatile boolean authorized;
    private final Connection manager;
    private final AtomicBoolean flushScheduled = new AtomicBoolean();
    private Channel channel;

    public ConnectionHandler(String id, ConnectionConfig connectionConfig, ClientBootstrap bootstrap, Connection manager) {
        this.id = id;
        this.connectionConfig = connectionConfig;
        this.bootstrap = bootstrap;
        this.manager = manager;

    }

    public void connect() {
        bootstrap.connect(connectionConfig.ip, connectionConfig.port).addListener((ChannelFutureListener) future -> {
            if (!future.isSuccess()) {
                authorized = false;
                manager.onClosed(this);
            } else {
                channel = future.channel();
                channel.pipeline().addLast("handler", ConnectionHandler.this);
                channel.writeAndFlush(new C2SHelloPacket(Packets.PROTOCOL_VERSION, id));
            }
        });
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Packet msg) throws Exception {
        if (!authorized) {
            if (msg instanceof S2CNoncePacket noncePacket) {
                if (!connectionConfig.keyPath.exists()){
                    disconnect(ctx, "Key not found " + connectionConfig.keyPath);
                    return;
                }
                PrivateKey key = Ed25519.privateKeyFromBase64(Files.readString(connectionConfig.keyPath.toPath()));
                var nonce = noncePacket.nonce();
                var idBytes = id.getBytes(StandardCharsets.UTF_8);
                byte[] payload = Arrays.copyOf(idBytes, idBytes.length + nonce.length);
                System.arraycopy(nonce, 0, payload, idBytes.length, nonce.length);
                byte[] signature = Ed25519.sign(key, payload);
                channel.writeAndFlush(new C2SLoginPacket(signature));
            } else if (msg instanceof S2CPostLoginPacket) {
                authorized = true;
                manager.postLogin(this);
            }
        } else {
            manager.onReceive(msg);
        }
    }

    public ChannelFuture writeAndFlush(Packet packet) {
        return channel.writeAndFlush(packet);
    }
    public void send(Packet packet) {
        channel.write(packet);
        if (flushScheduled.compareAndSet(false, true)) {
            channel.eventLoop().schedule(() -> {
                channel.flush();
                flushScheduled.set(false);
            }, 2, TimeUnit.MILLISECONDS);
        }
    }

    public boolean authorized() {
        return authorized && channel.isActive();
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
        if (!authorized){
            if (ctx.channel().isOpen()) {
                ctx.channel().close();
                log.info("Disconnect unauthorized connection {}, reason: {}", ctx.channel().remoteAddress(), message);
            }
        }
        authorized = false;
        manager.onClosed(this);
        if (ctx.channel().isOpen()) {
            ctx.channel().close();
        }
    }

    public void close() {
        var c = channel;
        if (c != null) {
            c.eventLoop().submit(() -> {
                c.flush();
                c.close();
            });
        }
    }
}
