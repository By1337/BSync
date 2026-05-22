package dev.by1337.sync.common.packet;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.DecoderException;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Function;

public interface ByteBufCodecs {

    static <T> @Nullable T readOptional(ByteBuf buf, Function<ByteBuf, T> reader) {
        if (buf.readBoolean()) {
            return reader.apply(buf);
        }
        return null;
    }

    static <T> void writeOptional(ByteBuf buf, @Nullable T t, BiConsumer<ByteBuf, T> writer) {
        if (t == null) {
            buf.writeBoolean(false);
        } else {
            buf.writeBoolean(true);
            writer.accept(buf, t);
        }
    }

    static void writeUtf8(ByteBuf buf, String str) {
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        buf.writeInt(bytes.length);
        buf.writeBytes(bytes);
    }

    static String readUtf8(ByteBuf buf) {
        int length = buf.readInt();
        if (length < 0 || length > Short.MAX_VALUE) throw new DecoderException("Invalid utf8 length: " + length);
        byte[] bytes = new byte[length];
        buf.readBytes(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    static void writeUUID(ByteBuf buf, UUID uuid) {
        buf.writeLong(uuid.getMostSignificantBits());
        buf.writeLong(uuid.getLeastSignificantBits());
    }

    static UUID readUUID(ByteBuf buf) {
        return new UUID(buf.readLong(), buf.readLong());
    }

    static void writeByteArray(ByteBuf buf, byte[] bytes) {
        buf.writeInt(bytes.length);
        buf.writeBytes(bytes);
    }

    static byte[] readByteArray(ByteBuf buf) {
        int length = buf.readInt();
        if (length < 0 || length >= 32 << 20) throw new DecoderException("Invalid array length: " + length);
        byte[] bytes = new byte[length];
        buf.readBytes(bytes);
        return bytes;
    }
}
