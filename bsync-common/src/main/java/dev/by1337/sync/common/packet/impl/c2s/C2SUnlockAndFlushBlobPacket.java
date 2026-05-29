package dev.by1337.sync.common.packet.impl.c2s;

import dev.by1337.sync.common.packet.ByteBufCodecs;
import dev.by1337.sync.common.packet.Packet;
import io.netty.buffer.ByteBuf;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

public record C2SUnlockAndFlushBlobPacket(UUID key, byte @Nullable [] blob, int token) implements Packet {

    public C2SUnlockAndFlushBlobPacket(ByteBuf buf, int protocolVersion) {
        this(
                ByteBufCodecs.readUUID(buf),
                ByteBufCodecs.readOptional(buf, ByteBufCodecs::readByteArray),
                buf.readInt()
        );
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        ByteBufCodecs.writeUUID(buf, key);
        ByteBufCodecs.writeOptional(buf, blob, ByteBufCodecs::writeByteArray);
        buf.writeInt(token);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        C2SUnlockAndFlushBlobPacket that = (C2SUnlockAndFlushBlobPacket) o;
        return token == that.token && Objects.equals(key, that.key) && Objects.deepEquals(blob, that.blob);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, Arrays.hashCode(blob), token);
    }

    @Override
    public String toString() {
        return "C2SUnlockAndFlushBlobPacket{" +
                "key=" + key +
                ", blob=" + (blob == null ? 0 : blob.length) + " bytes..." +
                ", token=" + token +
                '}';
    }
}
