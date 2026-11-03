package dev.by1337.sync.common.packet.impl.s2c;

import dev.by1337.sync.common.packet.*;
import dev.by1337.sync.common.transfer.TransferEntry;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.DecoderException;
import java.util.*;

public record S2CExportPagePacket(List<TransferEntry> entries, UUID afterKey, long afterSequence, boolean done, String error) implements Packet {
    public S2CExportPagePacket(ByteBuf buf, int version) {
        this(readEntries(buf), ByteBufCodecs.readOptional(buf, ByteBufCodecs::readUUID), buf.readLong(), buf.readBoolean(), ByteBufCodecs.readUtf8(buf));
    }

    public static S2CExportPagePacket empty() {
        return new S2CExportPagePacket(List.of(), null, 0, true, "");
    }

    public static S2CExportPagePacket failed(String error) {
        return new S2CExportPagePacket(List.of(), null, 0, true, error);
    }

    private static List<TransferEntry> readEntries(ByteBuf buf) {
        int count = buf.readInt();
        if (count < 0 || count > TransferEntry.PAGE_ROWS) throw new DecoderException("Invalid export page row count");
        var entries = new ArrayList<TransferEntry>(count);
        int bytes = 0;
        for (int i = 0; i < count; i++) {
            var kind = TransferEntry.Kind.values()[buf.readUnsignedByte()];
            UUID key = ByteBufCodecs.readUUID(buf);
            UUID id = ByteBufCodecs.readOptional(buf, ByteBufCodecs::readUUID);
            int length = buf.readInt();
            int max = kind == TransferEntry.Kind.DATA ? TransferEntry.MAX_DATA_BYTES : dev.by1337.sync.common.mail.Mail.MAX_PAYLOAD_BYTES;
            if (length < 0 || length > max || length > buf.readableBytes()
                    || (i > 0 && bytes + length > TransferEntry.PAGE_BYTES))
                throw new DecoderException("Oversized export page");
            byte[] payload = new byte[length];
            buf.readBytes(payload);
            entries.add(new TransferEntry(kind, key, payload, id));
            bytes += length;
        }
        return List.copyOf(entries);
    }

    @Override
    public void write(ByteBuf buf, int version) {
        buf.writeInt(entries.size());
        for (var entry : entries) {
            buf.writeByte(entry.kind().ordinal());
            ByteBufCodecs.writeUUID(buf, entry.key());
            ByteBufCodecs.writeOptional(buf, entry.messageId(), ByteBufCodecs::writeUUID);
            ByteBufCodecs.writeByteArray(buf, entry.payload());
        }
        ByteBufCodecs.writeOptional(buf, afterKey, ByteBufCodecs::writeUUID);
        buf.writeLong(afterSequence);
        buf.writeBoolean(done);
        ByteBufCodecs.writeUtf8(buf, error);
    }
}
