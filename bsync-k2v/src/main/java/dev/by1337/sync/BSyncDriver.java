/*
package dev.by1337.sync;

import dev.by1337.sync.bukkit.BSync;
import dev.by1337.sync.client.channel.ChannelMaker;
import dev.by1337.sync.client.channel.handler.lock.LockManager;
import dev.by1337.sync.client.channel.handler.lock.Locks;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;
import java.util.function.BiConsumer;

public class BSyncDriver implements LockDriver{
    private final String group;
    private final String serviceId;
    private final Locks locks;

    public BSyncDriver(String group, String serviceId) {
        this.group = group;
        this.serviceId = serviceId;
        locks =  ChannelMaker.createGroupLocks(BSync.getGroup(group), serviceId, new LockManager() {
            @Override
            public boolean ensureLockOwnership(UUID key) {
                return false;
            }

            @Override
            public void acceptMail(UUID key, String json) {

            }

            @Override
            public void forceUnlock(UUID key) {

            }

            @Override
            public void close() {

            }
        });
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
        return locks.lockAndLoadData(key, (s, a) -> callback.accept(s == Locks.LockStatus.SUCCESS, a));
    }
}
*/
