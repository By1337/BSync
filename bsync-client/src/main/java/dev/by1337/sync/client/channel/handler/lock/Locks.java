package dev.by1337.sync.client.channel.handler.lock;

import org.jetbrains.annotations.Nullable;

import java.util.UUID;
import java.util.function.BiConsumer;

public interface Locks {
    boolean isLocked(UUID uuid);

    void pushMail(UUID key, String json);

    void pushSnapshot(UUID key, byte[] snapshot);

    default void unlock(UUID key) {
        unlock(key, -1);
    }

    void unlock(UUID key, int version);

    int lockAndLoadData(UUID key, BiConsumer<LockStatus, byte @Nullable []> callback);

    enum LockStatus {
        SUCCESS,
        FAILURE
    }

}
