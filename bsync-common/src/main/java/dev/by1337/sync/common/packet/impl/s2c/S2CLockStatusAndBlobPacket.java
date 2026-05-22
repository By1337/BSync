package dev.by1337.sync.common.packet.impl.s2c;

import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.DecoderException;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

public final class S2CLockStatusAndBlobPacket implements Packet {

    public final Status status;
    public final byte @Nullable [] blob;

    public S2CLockStatusAndBlobPacket(Status status, byte @Nullable [] blob) {
        this.status = status;
        this.blob = blob;
    }

    public S2CLockStatusAndBlobPacket(ByteBuf buf, int protocolVersion) {
        var v = buf.readByte();
        if (v == 0) {
            status = Status.ACCEPTED;
        } else if (v == 1) {
            status = Status.REJECTED;
        } else {
            throw new DecoderException("Unknown mail status " + v);
        }
        if (buf.readBoolean()) {
            blob = new byte[buf.readInt()];
            buf.readBytes(blob);
        }else {
            blob = null;
        }
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

    @Override
    public String toString() {
        return "S2CLockStatusAndBlobPacket{" +
                "status=" + status +
                ", blob=" + Arrays.toString(blob) +
                '}';
    }
}
