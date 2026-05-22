package dev.by1337.sync.common.packet.impl.c2s;

import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.DecoderException;

public final class C2SLockStatusResponsePacket implements Packet {

    public final Status status;

    public C2SLockStatusResponsePacket(Status status) {
        this.status = status;
    }

    public C2SLockStatusResponsePacket(ByteBuf buf, int protocolVersion) {
        var v = buf.readByte();
        if (v == 0) {
            status = Status.LOCKED;
        } else if (v == 1) {
            status = Status.FREE;
        } else {
            throw new DecoderException("Unknown mail status " + v);
        }
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        buf.writeByte(status.id);
    }

    @Override
    public int getId() {
        return Packets.C2S_LOCK_STATUS_RESPONSE_PACKET;
    }

    public static C2SLockStatusResponsePacket free() {
        return new C2SLockStatusResponsePacket(Status.FREE);
    }
    public static C2SLockStatusResponsePacket locked() {
        return new C2SLockStatusResponsePacket(Status.LOCKED);
    }

    public boolean isFree() {
        return status == Status.FREE;
    }
    public boolean isLocked() {
        return status == Status.LOCKED;
    }


    public enum Status {
        LOCKED((byte) 0),
        FREE((byte) 1),
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
