package dev.by1337.sync.common.packet;

import dev.by1337.sync.common.packet.impl.*;
import dev.by1337.sync.common.packet.impl.c2s.*;
import dev.by1337.sync.common.packet.impl.s2c.*;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.DecoderException;

public class Packets {
    public static final int PROTOCOL_VERSION = 1;

    public static final int C2S_HELLO_PACKET = 0;
    public static final int S2C_NONCE_PACKET = 1;
    public static final int C2S_LOGIN_PACKET = 2;
    public static final int C2S_POST_LOGIN_PACKET = 3;
    public static final int CHANNELED_PACKET = 4;
    public static final int REQUEST_PACKET = 5;
    public static final int RESPONSE_PACKET = 6;
    public static final int PING_PACKET = 7;
    public static final int PONG_PACKET = 8;
    public static final int S2C_MAIL_ACCEPT_PACKET = 9;
    public static final int C2S_MAIL_STATUS_PACKET = 10;
    public static final int C2S_PUSH_MAIL_PACKET = 11;
    public static final int C2S_POLL_ALL_MAILS_PACKET = 12;
    public static final int S2C_LOCK_STATUS_REQUEST_PACKET = 13;
    public static final int C2S_LOCK_STATUS_RESPONSE_PACKET = 14;
    public static final int S2C_FORCE_UNLOCK_PACKET = 15;
    public static final int C2S_LOCK_AND_GET_BLOB_PACKET = 16;
    public static final int S2C_LOCK_STATUS_AND_BLOB_PACKET = 17;
    public static final int C2S_RELOCK_PACKET = 18;
    public static final int S2C_LOCK_STATUS_PACKET = 19;
    public static final int C2S_UNLOCK_AND_FLUSH_BLOB_PACKET = 20;
    public static final int C2S_OPEN_CHANNEL_PACKET = 21;
    public static final int C2S_CLOSE_CHANNEL_PACKET = 22;
    public static final int S2C_CHANNEL_STATUS_PACKET = 23;
    public static final int C2S_UNLOCK_PACKET = 24;
    public static final int C2S_LOCK_STATUS_REQUEST_PACKET = 25;


    public static Packet read(ByteBuf buf, int protocolVersion) throws DecoderException {
        if (buf.readableBytes() < 1) throw new DecoderException("Invalid packet length");
        int id = buf.readUnsignedByte();
        return switch (id) {
            case C2S_HELLO_PACKET -> new C2SHelloPacket().readAndGet(buf, protocolVersion);
            case S2C_NONCE_PACKET -> new S2CNoncePacket().readAndGet(buf, protocolVersion);
            case C2S_LOGIN_PACKET -> new C2SLoginPacket().readAndGet(buf, protocolVersion);
            case C2S_POST_LOGIN_PACKET -> new S2CPostLoginPacket().readAndGet(buf, protocolVersion);
            case CHANNELED_PACKET -> new ChanneledPacket().readAndGet(buf, protocolVersion);
            case REQUEST_PACKET -> new RequestPacket().readAndGet(buf, protocolVersion);
            case RESPONSE_PACKET -> new ResponsePacket().readAndGet(buf, protocolVersion);
            case PING_PACKET -> new PingPacket().readAndGet(buf, protocolVersion);
            case PONG_PACKET -> new PongPacket().readAndGet(buf, protocolVersion);
            case S2C_MAIL_ACCEPT_PACKET -> new S2CMailAcceptPacket().readAndGet(buf, protocolVersion);
            case C2S_MAIL_STATUS_PACKET -> new C2SMailResponsePacket().readAndGet(buf, protocolVersion);
            case C2S_PUSH_MAIL_PACKET -> new C2SPushMailPacket().readAndGet(buf, protocolVersion);
            case C2S_POLL_ALL_MAILS_PACKET -> new C2SPollAllMailsPacket().readAndGet(buf, protocolVersion);
            case S2C_LOCK_STATUS_REQUEST_PACKET -> new S2CLockStatusRequestPacket().readAndGet(buf, protocolVersion);
            case C2S_LOCK_STATUS_RESPONSE_PACKET -> new C2SLockStatusResponsePacket().readAndGet(buf, protocolVersion);
            case S2C_FORCE_UNLOCK_PACKET -> new S2CForceUnlockPacket().readAndGet(buf, protocolVersion);
            case C2S_LOCK_AND_GET_BLOB_PACKET -> new C2SLockAndGetBlobRequestPacket().readAndGet(buf, protocolVersion);
            case S2C_LOCK_STATUS_AND_BLOB_PACKET -> new S2CLockStatusAndBlobPacket().readAndGet(buf, protocolVersion);
            case C2S_RELOCK_PACKET -> new C2SRelockRequestPacket().readAndGet(buf, protocolVersion);
            case S2C_LOCK_STATUS_PACKET -> new S2CLockStatusResponsePacket().readAndGet(buf, protocolVersion);
            case C2S_UNLOCK_AND_FLUSH_BLOB_PACKET -> new C2SUnlockAndFlushBlobPacket().readAndGet(buf, protocolVersion);
            case C2S_OPEN_CHANNEL_PACKET -> new C2SOpenChannelPacket().readAndGet(buf, protocolVersion);
            case C2S_CLOSE_CHANNEL_PACKET -> new C2SCloseChannelPacket().readAndGet(buf, protocolVersion);
            case S2C_CHANNEL_STATUS_PACKET -> new S2CChannelStatsPacket().readAndGet(buf, protocolVersion);
            case C2S_UNLOCK_PACKET -> new C2SUnlockPacket().readAndGet(buf, protocolVersion);
            case C2S_LOCK_STATUS_REQUEST_PACKET -> new C2SLockStatusReqestPacket().readAndGet(buf, protocolVersion);
            default -> throw new DecoderException("Invalid packet id " + id);
        };
    }

    public static void write(ByteBuf buf, int protocolVersion, Packet packet) {
        buf.writeByte(packet.getId());
        packet.write(buf, protocolVersion);
    }
}
