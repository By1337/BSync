package dev.by1337.sync.common.packet.impl.c2s;

import dev.by1337.sync.common.packet.*;
import dev.by1337.sync.common.packet.impl.s2c.S2CImportResultPacket;
import dev.by1337.sync.common.transfer.TransferEntry;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.DecoderException;
import java.util.UUID;

public record C2SImportDataPacket(UUID key, byte[] data) implements Packet, ExpectsResponse<S2CImportResultPacket> {
    public C2SImportDataPacket(ByteBuf buf, int version) {
        this(ByteBufCodecs.readUUID(buf), readData(buf));
    }

    private static byte[] readData(ByteBuf buf) {
        int length = buf.readInt();
        if (length < 0 || length > TransferEntry.MAX_DATA_BYTES || length > buf.readableBytes()) throw new DecoderException("Invalid import data size");
        byte[] bytes = new byte[length];
        buf.readBytes(bytes);
        return bytes;
    }

    @Override
    public void write(ByteBuf buf, int version) {
        ByteBufCodecs.writeUUID(buf, key);
        ByteBufCodecs.writeByteArray(buf, data);
    }
}
