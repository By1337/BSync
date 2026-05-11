package dev.by1337.sync.server.network;

import dev.by1337.sync.common.netty.handler.PacketDecoder;
import dev.by1337.sync.common.netty.handler.PacketEncoder;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import dev.by1337.sync.common.packet.c2s.C2SHelloPacket;
import dev.by1337.sync.common.packet.c2s.C2SLoginPacket;
import dev.by1337.sync.common.packet.s2c.S2CNoncePacket;
import dev.by1337.sync.common.security.Ed25519;
import dev.by1337.sync.server.DedicatedServer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.ReadTimeoutHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;

public class LoginPacketListener extends SimpleChannelInboundHandler<ByteBuf> {
    private static final Logger log = LoggerFactory.getLogger(LoginPacketListener.class);
    private static final SecureRandom secureRandom = new SecureRandom();
    private int protocolVersion = Packets.PROTOCOL_VERSION;
    private State state = State.HELLO;
    private String id = "unknown";
    private final DedicatedServer server;
    private byte[] nonce;

    public LoginPacketListener(DedicatedServer server) {
        this.server = server;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf buf) throws Exception {
        Packet packet;
        try {
            packet = Packets.read(buf, protocolVersion);
        } finally {
            buf.release();
        }
        if (state == State.HELLO) {
            if (packet instanceof C2SHelloPacket hello) {
                protocolVersion = hello.protocol;
                id = hello.id;
                if (protocolVersion < 0 || protocolVersion > Packets.PROTOCOL_VERSION) {
                    disconnect(ctx, "Unsupported protocol version " + protocolVersion);
                    return;
                } else if (protocolVersion < Packets.PROTOCOL_VERSION) {
                    log.warn("Legacy protocol version {} {}[{}]", protocolVersion, id, ctx.channel().remoteAddress());
                }
                state = State.LOGIN;
                nonce = new byte[32];
                secureRandom.nextBytes(nonce);
                send(ctx, new S2CNoncePacket(nonce));
            } else {
                disconnect(ctx, "Unexpected packet type " + packet + " " + state);
            }
        } else if (state == State.LOGIN) {
            if (packet instanceof C2SLoginPacket login) {
                var idBytes = id.getBytes(StandardCharsets.UTF_8);
                byte[] payload = Arrays.copyOf(idBytes, idBytes.length + nonce.length);
                System.arraycopy(nonce, 0, payload, idBytes.length, nonce.length);
                byte[] signature = login.payload;
                boolean valid = false;
                for (PublicKey key : server.config().getAuthorizedKeys()) {
                    try {
                        if (Ed25519.verify(key, payload, signature)) {
                            valid = true;
                            break;
                        }
                    } catch (GeneralSecurityException ignored) {
                    }
                }
                if (!valid) {
                    disconnect(ctx, "Bad key");
                    return;
                }
                var pipeline = ctx.channel().pipeline();

                pipeline.replace("timeout", "timeout", new ReadTimeoutHandler(120));
                Connection connection = new Connection(ctx.channel(), server, id, protocolVersion);
                pipeline.replace("login", "auth", connection);
                pipeline.addAfter("splitter", "decoder", new PacketDecoder(protocolVersion));
                pipeline.addBefore("prepender", "encoder", new PacketEncoder(protocolVersion));
                server.clientList().addConnection(connection);
            } else {
                disconnect(ctx, "Unexpected packet type " + packet + " " + state);
            }
        }
    }

    private void send(ChannelHandlerContext ctx, Packet packet) {
        var buf = ctx.channel().alloc().ioBuffer();
        Packets.write(buf, protocolVersion, packet);
        ctx.channel().writeAndFlush(buf);
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
            log.info("Disconnect unauthorized connection {}[{}], reason: {}", id, ctx.channel().remoteAddress(), message);
        }
    }

    public enum State {
        HELLO,
        LOGIN,
    }
}
