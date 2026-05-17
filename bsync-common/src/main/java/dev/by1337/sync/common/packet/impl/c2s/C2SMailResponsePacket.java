package dev.by1337.sync.common.packet.impl.c2s;

import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.DecoderException;

public final class C2SMailResponsePacket implements Packet {

    public Status status;

    public C2SMailResponsePacket(Status status) {
        this.status = status;
    }

    public C2SMailResponsePacket() {
    }

    public static C2SMailResponsePacket reject() {
        return new C2SMailResponsePacket(Status.REJECTED);
    }
    public static C2SMailResponsePacket accepted() {
        return new C2SMailResponsePacket(Status.ACCEPTED);
    }


    @Override
    public void read(ByteBuf buf, int protocolVersion) {
        var v = buf.readByte();
        if (v == 0) {
            status = Status.ACCEPTED;
        } else if (v == 1) {
            status = Status.REJECTED;
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
        return Packets.C2S_MAIL_STATUS_PACKET;
    }

    public enum Status {
        ACCEPTED((byte) 0),
        REJECTED((byte) 1),
        ;
        private final byte id;

        Status(byte id) {
            this.id = id;
        }
    }

    @Override
    public String toString() {
        return "C2SMailResponsePacket{" +
                "status=" + status +
                '}';
    }
}
