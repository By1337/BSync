package dev.by1337.sync.common.packet;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.DecoderException;

import java.nio.charset.StandardCharsets;

public class ByteBufCodecs {

    public static void writeUtf8(ByteBuf buf, String str) {
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        buf.writeInt(bytes.length);
        buf.writeBytes(bytes);
    }
    public static String readUtf8(ByteBuf buf) {
        int length = buf.readInt();
        if (length < 0 || length > Short.MAX_VALUE) throw new DecoderException("Invalid utf8 length: " + length);
        byte[] bytes = new byte[length];
        buf.readBytes(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
