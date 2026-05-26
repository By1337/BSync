package dev.by1337.sync.common.packet.impl.c2s;

import dev.by1337.sync.common.packet.ByteBufCodecs;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

public record C2SFlushBlobPacket(UUID key, int token, int version,
                                 byte @Nullable [] blob) implements Packet {

    public C2SFlushBlobPacket(ByteBuf buf, int protocolVersion) {
        this(ByteBufCodecs.readUUID(buf), buf.readInt(), buf.readInt(), ByteBufCodecs.readOptional(buf, ByteBufCodecs::readByteArray));
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        ByteBufCodecs.writeUUID(buf, key);
        buf.writeInt(token);
        buf.writeInt(version);
        ByteBufCodecs.writeOptional(buf, blob, ByteBufCodecs::writeByteArray);
    }

    @Override
    public int getId() {
        return Packets.C2S_FLUSH_BLOB_REQUEST;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        C2SFlushBlobPacket that = (C2SFlushBlobPacket) o;
        return token == that.token && version == that.version && Objects.equals(key, that.key) && Objects.deepEquals(blob, that.blob);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, token, version, Arrays.hashCode(blob));
    }

    @Override
    public String toString() {
        return "C2SFlushBlobRequestPacket{" +
                "key=" + key +
                ", token=" + token +
                ", version=" + version +
                ", blob=" + (blob == null ? 0 : blob.length) + " bytes..." +
                '}';
    }
}
