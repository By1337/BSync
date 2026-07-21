package dev.by1337.sync.storage;

import org.jetbrains.annotations.Nullable;

import java.util.UUID;
import java.util.function.BiConsumer;

public interface PlayerDataStorage {
    void setLockValidator(O2BTester<UUID> tester);

    void doMailsLoad(UUID key);

    void setMailAccept(BiConsumer<UUID, String> accept);

    boolean isLocked(UUID uuid);

    void pushMail(UUID key, String json);

    void pushSnapshot(UUID key, byte[] snapshot);

    default void unlock(UUID key) {
        unlock(key, -1);
    }

    void unlock(UUID key, int version);

    int lockAndLoadData(UUID key, BiConsumer<Boolean, byte @Nullable []> callback);

    void close();

    @FunctionalInterface
    public interface O2BTester<T> {
        boolean test(T v);
    }
}
