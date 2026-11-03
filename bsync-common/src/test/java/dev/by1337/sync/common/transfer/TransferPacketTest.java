package dev.by1337.sync.common.transfer;

import dev.by1337.sync.common.packet.Packets;
import dev.by1337.sync.common.packet.impl.c2s.*;
import dev.by1337.sync.common.packet.impl.s2c.*;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.DecoderException;
import org.testng.annotations.Test;
import java.util.*;
import static org.testng.Assert.*;

public class TransferPacketTest {
    @Test
    public void pagePreservesBinaryDataEmptyMailAndCursors() {
        UUID key = UUID.randomUUID(), id = UUID.randomUUID();
        var page = new S2CExportPagePacket(List.of(new TransferEntry(TransferEntry.Kind.DATA, key, new byte[]{ 0, -1 }, null),
                new TransferEntry(TransferEntry.Kind.MAIL, key, new byte[0], id)), key, 123, false, "");
        var buffer = Unpooled.buffer();
        try {
            page.write(buffer, Packets.PROTOCOL_VERSION);
            var decoded = new S2CExportPagePacket(buffer, Packets.PROTOCOL_VERSION);
            assertEquals(decoded.afterKey(), key);
            assertEquals(decoded.afterSequence(), 123L);
            assertFalse(decoded.done());
            assertEquals(decoded.entries().get(0).payload(), new byte[]{ 0, -1 });
            assertEquals(decoded.entries().get(1).messageId(), id);
            assertEquals(decoded.entries().get(1).payload().length, 0);
            assertFalse(buffer.isReadable());
        } finally {
            buffer.release();
        }
    }

    @Test
    public void rejectsMaliciousPageCountAndImportLengthBeforeAllocating() {
        var count = Unpooled.buffer().writeInt(257);
        var length = Unpooled.buffer().writeLong(0).writeLong(0).writeInt(Integer.MAX_VALUE);
        try {
            assertThrows(DecoderException.class, () -> new S2CExportPagePacket(count, Packets.PROTOCOL_VERSION));
            assertThrows(DecoderException.class, () -> new C2SImportDataPacket(length, Packets.PROTOCOL_VERSION));
        } finally {
            count.release();
            length.release();
        }
    }

    @Test
    public void exportRequestPreservesAndValidatesShardParameters() {
        UUID key = UUID.randomUUID();
        var request = new C2SExportPagePacket(TransferEntry.Kind.MAIL, key, 123, 7, 4);
        var buffer = Unpooled.buffer();
        try {
            request.write(buffer, Packets.PROTOCOL_VERSION);
            assertEquals(new C2SExportPagePacket(buffer, Packets.PROTOCOL_VERSION), request);
            assertFalse(buffer.isReadable());
        } finally {
            buffer.release();
        }
        assertThrows(IllegalArgumentException.class, () -> new C2SExportPagePacket(TransferEntry.Kind.DATA, null, 0, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> new C2SExportPagePacket(TransferEntry.Kind.DATA, null, 0, 3, 3));
    }

}
