package dev.by1337.sync.common.packet.impl.s2c;

import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.DecoderException;

public record S2CLockStatusResponsePacket(Status status) implements Packet {
    public S2CLockStatusResponsePacket(ByteBuf buf, int protocolVersion) {
        this(switch (buf.readByte()){
            case 0 -> Status.OWNED;
            case 1 -> Status.REJECTED;
            default -> throw new DecoderException("Unknown lock status");
        });
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        buf.writeByte(status.id);
    }

    @Override
    public int getId() {
        return Packets.S2C_LOCK_STATUS_PACKET;
    }

    public static S2CLockStatusResponsePacket reject() {
        return new S2CLockStatusResponsePacket(Status.REJECTED);
    }

    public static S2CLockStatusResponsePacket owned() {
        return new S2CLockStatusResponsePacket(Status.OWNED);
    }

    public boolean isRejected() {
        return status == Status.REJECTED;
    }

    public boolean isOwned() {
        return status == Status.OWNED;
    }


    public enum Status {
        OWNED((byte) 0),
        REJECTED((byte) 1),
        ;
        private final byte id;

        Status(byte id) {
            this.id = id;
        }
    }
}
