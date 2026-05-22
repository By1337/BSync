package dev.by1337.sync.common.packet.impl.c2s;

import dev.by1337.sync.common.packet.ByteBufCodecs;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;

public final class C2SUnlockAndFlushBlobPacket implements Packet {
    public final UUID key;
    public final byte @Nullable [] blob;

    public C2SUnlockAndFlushBlobPacket(UUID key, byte @Nullable [] blob) {
        this.key = key;
        this.blob = blob;
    }

    public C2SUnlockAndFlushBlobPacket(ByteBuf buf, int protocolVersion) {
        key = ByteBufCodecs.readUUID(buf);
        if (buf.readBoolean()) {
            blob = new byte[buf.readInt()];
            buf.readBytes(blob);
        }else {
            blob = null;
        }
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        ByteBufCodecs.writeUUID(buf, key);
        if (blob != null) {
            buf.writeBoolean(true);
            buf.writeInt(blob.length);
            buf.writeBytes(blob);
        } else {
            buf.writeBoolean(false);
        }
    }

    @Override
    public int getId() {
        return Packets.C2S_UNLOCK_AND_FLUSH_BLOB_PACKET;
    }

    @Override
    public String toString() {
        return "C2SUnlockAndFlushBlobPacket{" +
                "key=" + key +
                '}';
    }
}
