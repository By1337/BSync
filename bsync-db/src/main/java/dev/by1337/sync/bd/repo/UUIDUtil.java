package dev.by1337.sync.bd.repo;

import java.util.UUID;

public class UUIDUtil {
    public static byte[] uuidToBytes(UUID uuid) {
        byte[] bytes = new byte[16];

        long most = uuid.getMostSignificantBits();
        long least = uuid.getLeastSignificantBits();

        for (int i = 0; i < 8; i++) {
            bytes[i] = (byte) (most >>> (56 - (i * 8)));
        }

        for (int i = 0; i < 8; i++) {
            bytes[i + 8] = (byte) (least >>> (56 - (i * 8)));
        }

        return bytes;
    }

    public static UUID bytesToUuid(byte[] bytes) {
        if (bytes.length != 16) {
            throw new IllegalArgumentException("UUID byte array must be 16 bytes");
        }

        long most = 0;
        long least = 0;

        for (int i = 0; i < 8; i++) {
            most = (most << 8) | (bytes[i] & 0xFFL);
        }

        for (int i = 8; i < 16; i++) {
            least = (least << 8) | (bytes[i] & 0xFFL);
        }

        return new UUID(most, least);
    }
}

