package dev.by1337.sync.client.channel.handler.lock;

import dev.by1337.sync.common.channel.ChannelType;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

public interface Locks {
    boolean isLocked(UUID uuid);

    void pushMail(UUID key, String json);

    void pushSnapshot(UUID key, byte[] snapshot);

    CompletableFuture<byte @Nullable []> loadSnapshot(UUID key);

    default void unlock(UUID key) {
        unlock(key, -1);
    }

    void unlock(UUID key, int version);

    int lockAndLoadData(UUID key, BiConsumer<LockStatus, byte @Nullable []> callback);

    boolean isReady();

    void loadMails(UUID key);

    enum LockStatus {
        SUCCESS,
        FAILURE
    }

    enum Type {
        ALL(ChannelType.LOCKS, true, true),
        ONLY_BLOBS(ChannelType.LOCKS_BLOBS_ONLY, false, true),
        ONLY_MAILBOX(ChannelType.LOCKS_MAILBOX_ONLY, true, false);
        public final String channelType;
        public final boolean hasMailbox;
        public final boolean hasBlobs;

        Type(String channelType, boolean hasMailbox, boolean hasBlobs) {
            this.channelType = channelType;
            this.hasMailbox = hasMailbox;
            this.hasBlobs = hasBlobs;
        }
    }
}
