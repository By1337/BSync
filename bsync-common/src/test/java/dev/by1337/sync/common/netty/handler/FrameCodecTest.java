package dev.by1337.sync.common.netty.handler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.TooLongFrameException;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class FrameCodecTest {
    @Test
    public void waitsForSplitHeaderAndPayload() {
        EmbeddedChannel channel = new EmbeddedChannel(new FrameDecoder());
        try {
            assertFalse(channel.writeInbound(Unpooled.buffer().writeShort(0)));
            assertFalse(channel.writeInbound(Unpooled.buffer().writeShort(3).writeByte(13)));
            assertNull(channel.readInbound());
            assertTrue(channel.writeInbound(Unpooled.wrappedBuffer(new byte[]{37, 42})));
            ByteBuf frame = channel.readInbound();
            try {
                assertEquals(frame.readableBytes(), 3);
                assertEquals(frame.readByte(), 13);
                assertEquals(frame.readByte(), 37);
                assertEquals(frame.readByte(), 42);
            } finally {
                frame.release();
            }
            assertNull(channel.readInbound());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void decodesCoalescedAndEmptyFrames() {
        EmbeddedChannel channel = new EmbeddedChannel(new FrameDecoder());
        try {
            assertTrue(channel.writeInbound(Unpooled.buffer()
                    .writeInt(1).writeByte(42).writeInt(0).writeInt(1).writeByte(13)));
            for (int expected : new int[]{42, -1, 13}) {
                ByteBuf frame = channel.readInbound();
                try {
                    assertEquals(frame.readableBytes(), expected == -1 ? 0 : 1);
                    if (expected != -1) assertEquals(frame.readByte(), expected);
                } finally {
                    frame.release();
                }
            }
            assertNull(channel.readInbound());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void rejectsNegativeAndOversizedLengths() {
        for (int length : new int[]{-1, FrameDecoder.MAX_FRAME_SIZE + 1}) {
            EmbeddedChannel channel = new EmbeddedChannel(new FrameDecoder());
            try {
                assertThrows(TooLongFrameException.class,
                        () -> channel.writeInbound(Unpooled.buffer().writeInt(length)));
            } finally {
                channel.finishAndReleaseAll();
            }
        }
    }

    @Test
    public void encoderUsesReadableRegionAndCountsHeaderBytes() {
        FrameEncoder encoder = new FrameEncoder();
        EmbeddedChannel channel = new EmbeddedChannel(encoder);
        try {
            ByteBuf payload = Unpooled.buffer().writeByte(99).writeByte(13).writeByte(37);
            payload.readByte();
            assertTrue(channel.writeOutbound(payload));
            ByteBuf header = channel.readOutbound();
            ByteBuf body = channel.readOutbound();
            try {
                assertEquals(header.readInt(), 2);
                assertEquals(header.readableBytes(), 0);
                assertEquals(body.readByte(), 13);
                assertEquals(body.readByte(), 37);
                assertEquals(encoder.totalBytesWrittenAndReset(), 6L);
                assertEquals(encoder.totalBytesWrittenAndReset(), 0L);
            } finally {
                header.release();
                body.release();
            }
            assertNull(channel.readOutbound());
        } finally {
            channel.finishAndReleaseAll();
        }
    }
}
