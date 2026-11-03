package dev.by1337.sync.k2v.storage;

import dev.by1337.sync.common.mail.Mail;
import dev.by1337.sync.common.transfer.TransferStorage;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface PlayerDataStorage extends TransferStorage {
    void setLockValidator(O2BTester<UUID> tester);

    default void setLockLostHandler(BiConsumer<UUID, Integer> handler) {
    }

    void doMailsLoad(UUID key);

    void setMailAccept(BiConsumer<UUID, String> accept);

    default void setMailHandler(Consumer<Mail> accept) {
        setMailAccept((key, payload) -> accept.accept(new Mail(UUID.randomUUID(), key, payload)));
    }

    boolean isLocked(UUID uuid);

    void pushMail(UUID key, String json);

    void pushSnapshot(UUID key, byte @Nullable [] snapshot);

    CompletableFuture<byte @Nullable []> loadSnapshot(UUID key);

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
