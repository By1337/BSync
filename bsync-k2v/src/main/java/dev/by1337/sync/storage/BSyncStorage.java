package dev.by1337.sync.storage;

import dev.by1337.sync.client.channel.ChannelMaker;
import dev.by1337.sync.client.channel.handler.lock.LockManager;
import dev.by1337.sync.client.channel.handler.lock.Locks;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;
import java.util.function.BiConsumer;

public class BSyncStorage implements PlayerDataStorage {
    private BiConsumer<UUID, String> mails;
    private Locks locks;
    private ChannelMaker.ChannelData<Locks> channel;
    private O2BTester<UUID> tester;

    public void setLocks(ChannelMaker.ChannelData<Locks> channel) {
        this.locks = channel.get();
        this.channel = channel;
    }

    @Override
    public void close() {
        channel.close();
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
    public void unlock(UUID key, int version) {
        locks.unlock(key, version);
    }

    @Override
    public int lockAndLoadData(UUID key, BiConsumer<Boolean, byte @Nullable []> callback) {
        return locks.lockAndLoadData(key, (s, p) -> callback.accept(s == Locks.LockStatus.SUCCESS, p));
    }
}
