package dev.by1337.sync.k2v.storage;

import dev.by1337.sync.client.channel.handler.lock.LockManager;
import dev.by1337.sync.client.channel.handler.lock.Locks;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

public class BSyncStorage implements PlayerDataStorage {
    private BiConsumer<UUID, String> mails;
    private Locks locks;
    private Runnable closer;
    private O2BTester<UUID> tester;

    public void setLocks(Locks locks, Runnable closer) {
        this.locks = locks;
        this.closer = closer;
    }

    @Override
    public void close() {
        closer.run();
    }

    public LockManager asBSyncLockManager() {
        return new LockManager() {
            @Override
            public boolean ensureLockOwnership(UUID key) {
                return tester.test(key);
            }

            @Override
            public void acceptMail(UUID key, String json) {
                mails.accept(key, json);
            }

            @Override
            public void forceUnlock(UUID key) {

            }

            @Override
            public void close() {

            }
        };
    }

    @Override
    public void setLockValidator(O2BTester<UUID> tester) {
        this.tester = tester;
    }

    @Override
    @Deprecated
    public void doMailsLoad(UUID key) {
        locks.loadMails(key);
    }

    @Override
    public void setMailAccept(BiConsumer<UUID, String> accept) {
        mails = accept;
    }

    @Override
    public boolean isLocked(UUID uuid) {
        return locks.isLocked(uuid);
    }

    @Override
    public void pushMail(UUID key, String json) {
        locks.pushMail(key, json);
    }

    @Override
    public void pushSnapshot(UUID key, byte[] snapshot) {
        locks.pushSnapshot(key, snapshot);
    }

    @Override
    public CompletableFuture<byte @Nullable []> loadSnapshot(UUID key) {
        return locks.loadSnapshot(key);
    }

    @Override
    public void unlock(UUID key, int version) {
        locks.unlock(key, version);
    }

    @Override
    public int lockAndLoadData(UUID key, BiConsumer<Boolean, byte @Nullable []> callback) {
        return locks.lockAndLoadData(key, (s, p) -> callback.accept(s == Locks.LockStatus.SUCCESS, p));
    }
}
