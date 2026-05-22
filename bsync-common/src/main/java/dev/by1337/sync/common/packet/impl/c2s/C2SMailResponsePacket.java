package dev.by1337.sync.common.packet.impl.c2s;

import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.DecoderException;

public record C2SMailResponsePacket(Status status) implements Packet {

    public C2SMailResponsePacket(ByteBuf buf, int protocolVersion) {
        this(switch (buf.readByte()) {
            case 0 -> Status.ACCEPTED;
            case 1 -> Status.REJECTED;
            default -> throw new DecoderException("Unknown mail status");
        });
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        buf.writeByte(status.id);
    }

    @Override
    public int getId() {
        return Packets.C2S_MAIL_STATUS_PACKET;
    }

    public static C2SMailResponsePacket reject() {
        return new C2SMailResponsePacket(Status.REJECTED);
    }

    public static C2SMailResponsePacket accepted() {
        return new C2SMailResponsePacket(Status.ACCEPTED);
    }

    public boolean isAccepted() {
        return status == Status.ACCEPTED;
    }

    public boolean isRejected() {
        return status == Status.REJECTED;
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
}
