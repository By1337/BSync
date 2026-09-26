package dev.by1337.sync.common.packet;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.DecoderException;
import org.testng.annotations.Test;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.*;

public class ByteBufCodecsTest {
    @Test
    public void preservesUnicodeEmptyStringsAndNullCharacters() {
        ByteBuf buf = Unpooled.buffer();
        try {
            for (String value : new String[]{"", "Hi", "a\0b", "x".repeat(Short.MAX_VALUE)}) {
                ByteBufCodecs.writeUtf8(buf, value);
                assertEquals(ByteBufCodecs.readUtf8(buf), value);
                assertEquals(buf.readableBytes(), 0);
            }
        } finally {
            buf.release();
        }
    }

    @Test
    public void rejectsInvalidLengthsBeforeReadingPayload() {
        for (int length : new int[]{-1, Short.MAX_VALUE + 1}) {
            ByteBuf buf = Unpooled.buffer().writeInt(length);
            try {
                assertThrows(DecoderException.class, () -> ByteBufCodecs.readUtf8(buf));
            } finally {
                buf.release();
            }
        }
        for (int length : new int[]{-1, 32 << 20}) {
            ByteBuf buf = Unpooled.buffer().writeInt(length);
            try {
                assertThrows(DecoderException.class, () -> ByteBufCodecs.readByteArray(buf));
            } finally {
                buf.release();
            }
        }
    }

    @Test
    public void rejectsTruncatedPayloads() {
        ByteBuf buf = Unpooled.buffer().writeInt(3).writeByte(42);
        try {
            assertThrows(IndexOutOfBoundsException.class, () -> ByteBufCodecs.readByteArray(buf));
            buf.readerIndex(0);
            assertThrows(IndexOutOfBoundsException.class, () -> ByteBufCodecs.readUtf8(buf));
        } finally {
            buf.release();
        }
    }

    @Test
    public void preservesUuidAndBinaryData() {
        ByteBuf buf = Unpooled.buffer();
        try {
            UUID uuid = new UUID(Long.MIN_VALUE, Long.MAX_VALUE);
            ByteBufCodecs.writeUUID(buf, uuid);
            assertEquals(ByteBufCodecs.readUUID(buf), uuid);
            for (byte[] data : new byte[][]{new byte[0], {0, -1, 13, 37}}) {
                ByteBufCodecs.writeByteArray(buf, data);
                assertEquals(ByteBufCodecs.readByteArray(buf), data);
            }
            assertEquals(buf.readableBytes(), 0);
        } finally {
            buf.release();
        }
    }

    @Test
    public void absentOptionalDoesNotInvokeCodec() {
        ByteBuf buf = Unpooled.buffer();
        AtomicInteger calls = new AtomicInteger();
        try {
            ByteBufCodecs.writeOptional(buf, null, (b, value) -> calls.incrementAndGet());
            assertNull(ByteBufCodecs.readOptional(buf, b -> calls.incrementAndGet()));
            assertEquals(calls.get(), 0);
            ByteBufCodecs.writeOptional(buf, "Привет", ByteBufCodecs::writeUtf8);
            assertEquals(ByteBufCodecs.readOptional(buf, ByteBufCodecs::readUtf8), "Привет");
            assertEquals(buf.readableBytes(), 0);
        } finally {
            buf.release();
        }
    }
}
