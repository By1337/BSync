package dev.by1337.sync.common.packet.impl.c2s;

import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.DecoderException;

public record C2SLockStatusResponsePacket(Status status, int st, int ct) implements Packet {

    public C2SLockStatusResponsePacket(ByteBuf buf, int protocolVersion) {
        this(switch (buf.readByte()){
            case 0 -> Status.LOCKED;
            case 1 -> Status.FREE;
            default -> throw  new DecoderException("Unknown mail status");
        }, buf.readInt(), buf.readInt());
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        buf.writeByte(status.id);
        buf.writeInt(st);
        buf.writeInt(ct);
    }

    @Override
    public int getId() {
        return Packets.C2S_LOCK_STATUS_RESPONSE_PACKET;
    }

    public static C2SLockStatusResponsePacket free(int st, int ct) {
        return new C2SLockStatusResponsePacket(Status.FREE, st, ct);
    }

    public static C2SLockStatusResponsePacket locked(int st, int ct) {
        return new C2SLockStatusResponsePacket(Status.LOCKED, st, ct);
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
}
