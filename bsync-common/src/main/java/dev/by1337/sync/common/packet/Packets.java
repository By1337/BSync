package dev.by1337.sync.common.packet;

import dev.by1337.sync.common.packet.impl.*;
import dev.by1337.sync.common.packet.impl.c2s.*;
import dev.by1337.sync.common.packet.impl.s2c.*;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.DecoderException;

public class Packets {
    public static final int PROTOCOL_VERSION = 2;
    public static final int LAST_SUPPORTED_VERSION = 2;

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
    public static final int S2C_FORCE_UNLOCK_PACKET = 13;
    public static final int C2S_LOCK_AND_GET_BLOB_PACKET = 14;
    public static final int S2C_LOCK_STATUS_AND_BLOB_PACKET = 15;
    public static final int C2S_UNLOCK_AND_FLUSH_BLOB_PACKET = 16;
    public static final int C2S_OPEN_CHANNEL_PACKET = 17;
    public static final int C2S_CLOSE_CHANNEL_PACKET = 18;
    public static final int S2C_CHANNEL_STATUS_PACKET = 19;
    public static final int C2S_UNLOCK_PACKET = 20;
    public static final int C2S_RENEW_LOCK_PACKET = 21;
    public static final int C2S_FLUSH_BLOB_REQUEST = 22;
    public static final int S2C_FLUSH_ACCEPT_RESPONSE = 23;
    public static final int ACK_REQUEST = 24;
    public static final int ACK_RESPONSE = 25;


    public static Packet read(ByteBuf buf, int protocolVersion) throws DecoderException {
        if (buf.readableBytes() < 1) throw new DecoderException("Invalid packet length");
        int id = buf.readUnsignedByte();
        return switch (id) {
            case C2S_HELLO_PACKET -> new C2SHelloPacket(buf, protocolVersion);
            case S2C_NONCE_PACKET -> new S2CNoncePacket(buf, protocolVersion);
            case C2S_LOGIN_PACKET -> new C2SLoginPacket(buf, protocolVersion);
            case C2S_POST_LOGIN_PACKET -> new S2CPostLoginPacket(buf, protocolVersion);
            case CHANNELED_PACKET -> new ChanneledPacket(buf, protocolVersion);
            case REQUEST_PACKET -> new RequestPacket(buf, protocolVersion);
            case RESPONSE_PACKET -> new ResponsePacket(buf, protocolVersion);
            case PING_PACKET -> new PingPacket(buf, protocolVersion);
            case PONG_PACKET -> new PongPacket(buf, protocolVersion);
            case S2C_MAIL_ACCEPT_PACKET -> new S2CMailAcceptPacket(buf, protocolVersion);
            case C2S_MAIL_STATUS_PACKET -> new C2SMailResponsePacket(buf, protocolVersion);
            case C2S_PUSH_MAIL_PACKET -> new C2SPushMailPacket(buf, protocolVersion);
            case C2S_POLL_ALL_MAILS_PACKET -> new C2SPollAllMailsPacket(buf, protocolVersion);
            case S2C_FORCE_UNLOCK_PACKET -> new S2CForceUnlockPacket(buf, protocolVersion);
            case C2S_LOCK_AND_GET_BLOB_PACKET -> new C2SLockAndGetBlobRequestPacket(buf, protocolVersion);
            case S2C_LOCK_STATUS_AND_BLOB_PACKET -> new S2CLockStatusAndBlobPacket(buf, protocolVersion);
            case C2S_UNLOCK_AND_FLUSH_BLOB_PACKET -> new C2SUnlockAndFlushBlobPacket(buf, protocolVersion);
            case C2S_OPEN_CHANNEL_PACKET -> C2SOpenChannelPacket.read(buf, protocolVersion);
            case C2S_CLOSE_CHANNEL_PACKET -> new C2SCloseChannelPacket(buf, protocolVersion);
            case S2C_CHANNEL_STATUS_PACKET -> new S2CChannelStatsPacket(buf, protocolVersion);
            case C2S_UNLOCK_PACKET -> new C2SUnlockPacket(buf, protocolVersion);
            case C2S_RENEW_LOCK_PACKET -> new C2SRenewLockPacket(buf, protocolVersion);
            case C2S_FLUSH_BLOB_REQUEST -> new C2SFlushBlobPacket(buf, protocolVersion);
            case S2C_FLUSH_ACCEPT_RESPONSE -> new S2CFlushResponsePacket(buf, protocolVersion);
            case ACK_REQUEST -> new AckRequest(buf, protocolVersion);
            case ACK_RESPONSE -> AckRequest.AckResponse.INSTANCE;
            default -> throw new DecoderException("Invalid packet id " + id);
        };
    }

    public static void write(ByteBuf buf, int protocolVersion, Packet packet) {
        buf.writeByte(packet.getId());
        packet.write(buf, protocolVersion);
    }

    public static boolean isSupportedProtocol(int version){
        return version >= LAST_SUPPORTED_VERSION;
    }
    public static boolean isLegacyProtocol(int version){
        return version < PROTOCOL_VERSION;
    }
}
