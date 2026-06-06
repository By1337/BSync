package dev.by1337.sync.server.channel.handler.log;


import java.nio.ByteBuffer;

public class ByteBufUtil {

    public static void writeVarInt1(ByteBuffer buf, int value) {
        buf.put((byte) value);
    }

    public static void writeVarInt2(ByteBuffer buf, int value) {
        buf.putShort((short) ((value & 0x7F | 0x80) << 8 | (value >>> 7)));
    }

    public static void writeVarInt(ByteBuffer buf, int value) {
        if ((value & (0xFFFFFFFF << 7)) == 0) {
            buf.put((byte) value);
        } else if ((value & (0xFFFFFFFF << 14)) == 0) {
            int w = (value & 0x7F | 0x80) << 8 | (value >>> 7);
            buf.putShort((short) w);
        } else {
            writeVarIntFull(buf, value);
        }
    }

    public static void writeVarIntFull(ByteBuffer buf, int value) {
        if ((value & (0xFFFFFFFF << 7)) == 0) {
            buf.put((byte) value);
        } else if ((value & (0xFFFFFFFF << 14)) == 0) {
            int w = (value & 0x7F | 0x80) << 8 | (value >>> 7);
            buf.putShort((short) w);
        } else if ((value & (0xFFFFFFFF << 21)) == 0) {
            int w = (value & 0x7F | 0x80) << 16 | ((value >>> 7) & 0x7F | 0x80) << 8 | (value >>> 14);
            buf.put((byte) (w >> 16));
            buf.put((byte) (w >> 8));
            buf.put((byte) w);
        } else if ((value & (0xFFFFFFFF << 28)) == 0) {
            int w = (value & 0x7F | 0x80) << 24 | (((value >>> 7) & 0x7F | 0x80) << 16)
                    | ((value >>> 14) & 0x7F | 0x80) << 8 | (value >>> 21);
            buf.putInt(w);
        } else {
            int w = (value & 0x7F | 0x80) << 24 | ((value >>> 7) & 0x7F | 0x80) << 16
                    | ((value >>> 14) & 0x7F | 0x80) << 8 | ((value >>> 21) & 0x7F | 0x80);
            buf.putInt(w);
            buf.put((byte) (value >>> 28));
        }
    }

    public static int varInt3Size(int value) {
        if ((value & (0xFFFFFFFF << 7)) == 0) {
            return 1;
        } else if ((value & (0xFFFFFFFF << 14)) == 0) {
            return 2;
        } else {
            return 3;
        }
    }
    public static int readVarInt(ByteBuffer buf) {
        int readable = buf.remaining();
        if (readable == 0) {
            return -1;
        }
        int k = buf.get();
        if ((k & 0x80) != 128) {
            return k;
        }
        int maxRead = Math.min(5, readable);
        int i = k & 0x7F;
        for (int j = 1; j < maxRead; j++) {
            k = buf.get();
            i |= (k & 0x7F) << j * 7;
            if ((k & 0x80) != 128) {
                return i;
            }
        }
        return -1;
    }
}