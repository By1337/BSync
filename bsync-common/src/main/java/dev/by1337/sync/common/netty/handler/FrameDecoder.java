package dev.by1337.sync.common.netty.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.TooLongFrameException;

import java.util.List;

public class FrameDecoder extends ByteToMessageDecoder {
    public static final int MAX_FRAME_SIZE = 32 * 1024 * 1024;


    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (in.readableBytes() < 4) {
            return;
        }

        in.markReaderIndex();
        int length = in.readInt();

        if (length < 0 || length > MAX_FRAME_SIZE) {
            throw new TooLongFrameException("Frame too large: " + length);
        }

        if (in.readableBytes() < length) {
            in.resetReaderIndex();
            return;
        }

        out.add(in.readBytes(length));
    }
}