package dev.by1337.sync.common.packet.impl.s2c;

import dev.by1337.sync.common.packet.ByteBufCodecs;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.DecoderException;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Objects;

public record S2CLockStatusAndBlobPacket(Status status, byte @Nullable [] blob, int token, int version) implements Packet {

    public S2CLockStatusAndBlobPacket(ByteBuf buf, int protocolVersion) {
        this(switch (buf.readByte()) {
            case 0 -> Status.ACCEPTED;
            case 1 -> Status.REJECTED;
            default -> throw new DecoderException("Unknown lock status");
        },
                ByteBufCodecs.readOptional(buf, ByteBufCodecs::readByteArray),
                buf.readInt(),
                buf.readInt()
        );
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
        buf.writeInt(token);
        buf.writeInt(version);
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

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        S2CLockStatusAndBlobPacket that = (S2CLockStatusAndBlobPacket) o;
        return token == that.token && version == that.version && status == that.status && Objects.deepEquals(blob, that.blob);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, Arrays.hashCode(blob), token, version);
    }

    @Override
    public String toString() {
        return "S2CLockStatusAndBlobPacket{" +
                "status=" + status +
                ", blob=" + (blob == null ? 0 : blob.length) + " bytes..." +
                ", token=" + token +
                ", version=" + version +
                '}';
    }
}
