package dev.by1337.sync.client.channel.handler.lock;

import java.util.UUID;

public interface LockManager {
    boolean ensureLockOwnership(UUID key);

    void acceptMail(UUID key, String json);

    default void acceptMail(UUID key, String json, UUID messageId) {
        acceptMail(key, json);
    }

    void forceUnlock(UUID key);

    void close();

}
