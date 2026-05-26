package dev.by1337.sync.client.channel.handler;

import java.util.UUID;

public interface LockManager {
    boolean ensureLockOwnership(UUID key);

    void acceptMail(UUID key, String json);
    void forceUnlock(UUID key);
    void close();
}
