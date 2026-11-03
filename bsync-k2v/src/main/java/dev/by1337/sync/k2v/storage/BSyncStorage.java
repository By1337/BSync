package dev.by1337.sync.k2v.storage;

import dev.by1337.sync.client.channel.handler.lock.LockManager;
import dev.by1337.sync.client.channel.handler.lock.Locks;
import dev.by1337.sync.k2v.mail.Mail;
import dev.by1337.sync.k2v.transfer.ExportSource;
import dev.by1337.sync.k2v.transfer.ExportPage;
import dev.by1337.sync.k2v.transfer.TransferEntry;
import dev.by1337.sync.k2v.transfer.TransferResult;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class BSyncStorage implements PlayerDataStorage {
    private Consumer<Mail> mails;
    private Locks locks;
    private Runnable closer;
    private O2BTester<UUID> tester;
    private BiConsumer<UUID, Integer> lockLost = (key, version) -> {
    };

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
                acceptMail(key, json, UUID.randomUUID());
            }

            @Override
            public void acceptMail(UUID key, String json, UUID messageId) {
                mails.accept(new Mail(messageId, key, json));
            }

            @Override
            public void forceUnlock(UUID key) {
                lockLost.accept(key, -1);
            }

            @Override
            public void close() {

            }
        };
    }

    @Override
    public void setLockLostHandler(BiConsumer<UUID, Integer> handler) {
        lockLost = handler;
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
        mails = mail -> accept.accept(mail.owner(), mail.payload());
    }

    @Override
    public void setMailHandler(Consumer<Mail> accept) {
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
    public List<ExportSource> exportSources() {
        return locks.exportSources().stream().map(source -> (ExportSource) new ExportSource() {
            @Override
            public CompletableFuture<ExportPage> next(TransferEntry.Kind kind) {
                return source.next(dev.by1337.sync.common.transfer.TransferEntry.Kind.valueOf(kind.name())).thenApply(page -> {
                    var entries = page.entries().stream().map(entry -> new TransferEntry(
                            TransferEntry.Kind.valueOf(entry.kind().name()), entry.key(), entry.payload(), entry.messageId())).toList();
                    return new ExportPage(entries, page.afterKey(), page.afterSequence(), page.done(), page.error());
                });
            }

            @Override
            public void close() throws Exception {
                source.close();
            }
        }).toList();
    }

    @Override
    public CompletableFuture<TransferResult> importEntry(TransferEntry entry) {
        var request = new dev.by1337.sync.common.transfer.TransferEntry(
                dev.by1337.sync.common.transfer.TransferEntry.Kind.valueOf(entry.kind().name()), entry.key(), entry.payload(), entry.messageId());
        return locks.importEntry(request).thenApply(result -> new TransferResult(
                TransferResult.Status.valueOf(result.status().name()), result.detail()));
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
