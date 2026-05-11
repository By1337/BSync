package dev.by1337.sync.common.netty.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.TooLongFrameException;

public class FrameEncoder extends ChannelOutboundHandlerAdapter {

    public void write(ChannelHandlerContext ctx, Object in, ChannelPromise promise) throws Exception {
        if (in instanceof ByteBuf msg) {
            int readableBytes = msg.readableBytes();
            if (readableBytes > FrameDecoder.MAX_FRAME_SIZE) {
                msg.release();
                throw new TooLongFrameException("Too long Frame " + readableBytes + " bytes");
            }
            var size = ctx.channel().alloc().ioBuffer(4, 4);
            size.writeInt(readableBytes);
            ctx.write(size, ctx.voidPromise());
            ctx.write(msg, promise);
        } else {
            ctx.write(in, promise);
        }
    }
}