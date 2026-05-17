package dev.by1337.sync.common.packet.impl.c2s;

import dev.by1337.sync.common.packet.ByteBufCodecs;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;

import java.util.UUID;

public final class C2SUnlockAndFlushBlobPacket implements Packet {
    public UUID key;
    public byte[] blob;

    public C2SUnlockAndFlushBlobPacket(UUID key, byte[] blob) {
        this.key = key;
        this.blob = blob;
    }

    public C2SUnlockAndFlushBlobPacket() {
    }

    @Override
    public void read(ByteBuf buf, int protocolVersion) {
        key = ByteBufCodecs.readUUID(buf);
        byte[] blob = new byte[buf.readInt()];
        buf.readBytes(blob);
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        ByteBufCodecs.writeUUID(buf, key);
        buf.writeInt(blob.length);
        buf.writeBytes(blob);
    }

    @Override
    public int getId() {
        return Packets.C2S_UNLOCK_AND_FLUSH_BLOB_PACKET;
    }

    @Override
    public String toString() {
        return "C2SLockAndGetBlobRequestPacket{" +
                "key=" + key +
                '}';
    }
}
