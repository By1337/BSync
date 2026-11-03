package dev.by1337.sync.common.transfer;

import java.util.UUID;

/** The same signed 64-bit XOR and floorMod used by the client's group router. */
public record ShardSelection(int count, int index) {
    public static final ShardSelection ALL = new ShardSelection(1, 0);

    public ShardSelection {
        if (count < 1 || index < 0 || index >= count)
            throw new IllegalArgumentException("Invalid shard selection: count=" + count + ", index=" + index);
    }

    public static long hash(UUID key) {
        return key.getMostSignificantBits() ^ key.getLeastSignificantBits();
    }

    public static int index(UUID key, int count) {
        return Math.floorMod(hash(key), count);
    }

    public boolean includes(UUID key) {
        return index(key, count) == index;
    }
}
