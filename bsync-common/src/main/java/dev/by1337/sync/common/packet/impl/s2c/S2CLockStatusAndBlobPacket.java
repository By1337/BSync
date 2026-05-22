package dev.by1337.sync.common.packet.impl.s2c;

import dev.by1337.sync.common.packet.ByteBufCodecs;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.DecoderException;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

public record S2CLockStatusAndBlobPacket(Status status, byte @Nullable [] blob) implements Packet {

    public S2CLockStatusAndBlobPacket(ByteBuf buf, int protocolVersion) {
        this(switch (buf.readByte()) {
            case 0 -> Status.ACCEPTED;
            case 1 -> Status.REJECTED;
            default -> throw new DecoderException("Unknown lock status");
        }, ByteBufCodecs.readOptional(buf, ByteBufCodecs::readByteArray));
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        buf.writeByte(status.id);
        if (blob != null) {
            buf.writeBoolean(true);
            buf.writeInt(blob.length);
            buf.writeBytes(blob);
        } else {
            buf.writeBoolean(false);
        }
    }

    public boolean isAccepted() {
        return status == Status.ACCEPTED;
    }

    public boolean isRejected() {
        return status == Status.REJECTED;
    }

    @Override
    public int getId() {
        return Packets.S2C_LOCK_STATUS_AND_BLOB_PACKET;
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
