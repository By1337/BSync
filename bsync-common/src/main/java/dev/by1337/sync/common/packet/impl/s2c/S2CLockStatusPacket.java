package dev.by1337.sync.common.packet.impl.s2c;

import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.DecoderException;

public final class S2CLockStatusPacket implements Packet {

    public Status status;

    public S2CLockStatusPacket(Status status) {
        this.status = status;
    }

    public S2CLockStatusPacket() {
    }

    public static S2CLockStatusPacket reject() {
        return new S2CLockStatusPacket(Status.REJECTED);
    }
    public static S2CLockStatusPacket owned() {
        return new S2CLockStatusPacket(Status.OWNED);
    }
    public boolean isRejected() {
        return status == Status.REJECTED;
    }
    public boolean isOwned() {
        return status == Status.OWNED;
    }


    @Override
    public void read(ByteBuf buf, int protocolVersion) {
        var v = buf.readByte();
        if (v == 0) {
            status = Status.OWNED;
        } else if (v == 1) {
            status = Status.REJECTED;
        } else {
            throw new DecoderException("Unknown lock status " + v);
        }
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        buf.writeByte(status.id);
    }

    @Override
    public int getId() {
        return Packets.S2C_LOCK_STATUS_PACKET;
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

    @Override
    public String toString() {
        return "S2CLockStatusPacket{" +
                "status=" + status +
                '}';
    }
}
