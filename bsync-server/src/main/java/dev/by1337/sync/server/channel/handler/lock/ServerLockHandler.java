package dev.by1337.sync.server.channel.handler.lock;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import dev.by1337.sync.common.channel.ChannelMessage;
import dev.by1337.sync.common.channel.handler.request.IncomingRequest;
import dev.by1337.sync.common.channel.pipeline.*;
import dev.by1337.sync.common.packet.impl.c2s.*;
import dev.by1337.sync.common.packet.impl.s2c.S2CForceUnlockPacket;
import dev.by1337.sync.common.packet.impl.s2c.S2CLockStatusAndBlobPacket;
import dev.by1337.sync.common.packet.impl.s2c.S2CMailAcceptPacket;
import dev.by1337.sync.common.util.BSUtils;
import dev.by1337.sync.common.work.EventLoopWorker;
import dev.by1337.sync.server.DedicatedServer;
import dev.by1337.sync.server.channel.ServerChannelRuntime;
import dev.by1337.sync.server.channel.messages.ClientDisconnectMessage;
import dev.by1337.sync.server.database.table.BatchedK2VCache;
import dev.by1337.sync.server.database.table.BatchedMailbox;
import dev.by1337.sync.server.database.table.MailboxRepository;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ServerLockHandler implements ChannelHandler {


    private final LockMap lockMap = new LockMap();

    private Logger log = DEFAULT_LOGGER;
    private EventLoopWorker eventLoop;
    private Pipeline pipeline;
    private ServerChannelRuntime serverChannel;
    private boolean closing = false;
    private BatchedK2VCache<UUID, byte[]> blobRepository;
    private DedicatedServer server;
    private MailBox mailBox;

    @Override
    public void init(ChannelRuntime r) {
        if (!(r instanceof ServerChannelRuntime runtime))
            throw new IllegalArgumentException("runtime must be a ServerChannelRuntime");
        if (this.eventLoop != null) {
            throw new IllegalStateException("Duplicate handler add!");
        }
        this.eventLoop = runtime.eventLoop();
        this.log = runtime.logger();
        pipeline = runtime.pipeline();
        serverChannel = runtime;
        eventLoop.schedule(this::tick, 10_000);
        server = runtime.server();
        mailBox = new MailBox(new BatchedMailbox(
                new MailboxRepository(
                        server.database().dataSource(),
                        runtime.channel().id() + "_mailbox_repository"
                ),
                DedicatedServer.IO_WORKERS.getNext()
        ));

        blobRepository = new BatchedK2VCache<>(
                new dev.by1337.sync.bd.repo.UUID2MediumBLOBRepository(server.database().dataSource(), runtime.channel().id() + "_blob_repository"),
                DedicatedServer.IO_WORKERS.getNext(),
                b -> b
                        .maximumWeight(1024 * 1024 * 1024)
                        .weigher((k, v) -> v.length + 16)
        );
    }

    private void tick() {
        long now = System.currentTimeMillis();
        for (var lock : List.copyOf(lockMap.key2lock.values())) {
            if (lock.lastConfirm + LockMap.OUTDATE_TIME_MS < now) {
                lockMap.unlock(lock.key);
                var v = serverChannel.lookup(lock.owner);
                if (v != null) v.write(new S2CForceUnlockPacket(lock.key, lock.token));
            }
        }
        eventLoop.schedule(this::tick, 15_000);
    }

    private <T> T safe(ESupplier<T> s) {
        try {
            return s.get();
        } catch (Exception e) {
            log.error("Failed to safe run!", e);
            return null;
        }
    }

    private void safe(ERunnable s) {
        try {
            s.run();
        } catch (Exception e) {
            log.error("Failed to safe run!", e);
        }
    }

    private <T> T safeOptional(ESupplier<Optional<T>> s) {
        try {
            return s.get().orElse(null);
        } catch (Exception e) {
            log.error("Failed to safe run!", e);
            return null;
        }
    }

    @FunctionalInterface
    public interface ESupplier<T> {
        T get() throws Exception;
    }

    @FunctionalInterface
    public interface ERunnable {
        void run() throws Exception;
    }


    @Override
    public void handle(ChannelContext ctx, ChannelMessage msg) {
        switch (msg) {
            case ClientDisconnectMessage(SocketConnection connection) -> {
                lockMap.markDisconnected(connection);
                ctx.fire(msg);
            }
            case IncomingRequest request -> {
                ChannelMessage payload = request.payload();
                if (payload instanceof C2SLockAndGetBlobRequestPacket r) {
                    // todo badShutdown тут не играет роли пока не научимся нормально закрывать соединения с серверной стороны
                    if (!r.recovery() && /*server.badShutdown() &&*/ server.uptimeMillis() < 3_000) {
                        log.warn("Guard time for key {}", r.key());
                        pipeline.schedule(msg, ctx.connection(), 500);
                        return;
                    }
                    var lock = lockMap.tryLock(r.key(), ctx.connection().transport());
                    if (lock == null) {
                        if (++request.counter >= 10) {
                            request.response(r, new S2CLockStatusAndBlobPacket(
                                    S2CLockStatusAndBlobPacket.Status.REJECTED, null, -1, r.version())
                            );
                        } else {
                            pipeline.schedule(msg, ctx.connection(), 500);
                        }
                    } else {
                        request.response(r, new S2CLockStatusAndBlobPacket(
                                        S2CLockStatusAndBlobPacket.Status.ACCEPTED,
                                        r.recovery() ? null :
                                                safeOptional(() -> blobRepository.get(r.key())),
                                        lock.token,
                                        r.version()
                                )
                        );
                    }
                } else {
                    ctx.fire(msg);
                }
            }
            case C2SRenewLockPacket(UUID key, int token) -> {
                var lock = lockMap.getLock(key);
                if (lock != null && lock.isOwner(token, ctx.connection().transport())) {
                    lock.lastConfirm = System.currentTimeMillis();
                } else {
                    ctx.connection().write(new S2CForceUnlockPacket(key, token));
                }
            }
            case C2SPollAllMailsPacket(UUID key, int token) -> {
                if (lockMap.isOwner(key, ctx.connection().transport(), token)) {
                    sendMail(ctx.connection(), key);
                }
            }
            case C2SPushMailPacket(UUID key, String json) -> {
                MailboxRepository.Mail mail = new MailboxRepository.Mail(mailBox.nextMailId(), key, json);
                mailBox.addMail(mail);
                var lock = lockMap.getLock(key);
                if (lock != null) {
                    sendMail(serverChannel.lookup(lock.owner), key);
                }
            }
            case C2SFlushBlobPacket flush -> {
                var lock = lockMap.getLock(flush.key());
                if (lock != null && lock.isOwner(flush.token(), ctx.connection().transport()) && flush.version() > lock.snapshotVersion) {
                    lock.snapshotVersion = flush.version();
                    var blob = flush.blob();
                    if (blob != null) {
                        safe(() -> blobRepository.put(lock.key, flush.blob()));
                    }
                }
            }
            case C2SUnlockPacket unlock ->
                    lockMap.unlockIfOwner(unlock.key(), ctx.connection().transport(), unlock.token());
            case null, default -> ctx.fire(msg);
        }
    }


    private void sendMail(Connection connection, UUID key) {
        var lock = lockMap.getLock(key);
        if (lock == null || lock.owner != connection.transport()) return;
        var mail = mailBox.peekNextMail(key);
        if (mail == null) return;
        new S2CMailAcceptPacket(key, mail.payload(), lock.token).request(pipeline, connection)
                .then((result) -> {
                    if (result instanceof C2SMailResponsePacket response) {
                        if (!lockMap.isOwner(key, connection.transport(), response.token())) {
                            log.error("Клиент {} принял mail без блокировки! {} {}", connection.transport(), mail, response);
                            return;
                        }
                        if (response.isAccepted()) {
                            mailBox.removeMail(mail);
                            sendMail(connection, key);
                        }
                    } else {
                        log.error("Client {} не ответил на S2CMailAcceptPacket {}", connection, result);
                    }
                });
    }

    @Override
    public void close() {
        closing = true;
        BSUtils.safe(() -> blobRepository.close());
        BSUtils.safe(() -> mailBox.close());
    }

    public static class LockData {
        public boolean disconnected;
        public long disconnectedAt;
        public int token;
        public final UUID key;
        public final SocketConnection owner;
        public long lastConfirm;
        public int snapshotVersion;

        public LockData(UUID key, SocketConnection owner, int token) {
            this.key = key;
            this.owner = owner;
            this.token = token;
        }

        public boolean isOwner(int token, SocketConnection c) {
            return this.token == token && c == owner;
        }
    }

    public static class LockMap {
        private final AtomicInteger counter = new AtomicInteger();
        private static final long OUTDATE_TIME_MS = 15_000;
        private final Map<SocketConnection, Set<UUID>> client2keys = new IdentityHashMap<>(1024);
        private final Map<UUID, SocketConnection> key2client = new HashMap<>(1024);
        private final Map<UUID, LockData> key2lock = new HashMap<>(1024);

        public @Nullable LockData getLock(UUID key) {
            return key2lock.get(key);
        }

        public boolean isOwner(UUID key, SocketConnection connection, int token) {
            var lock = key2lock.get(key);
            return lock != null && lock.owner == connection && lock.token == token;
        }

        public Set<UUID> getOwnedKeys(SocketConnection key) {
            return Collections.unmodifiableSet(client2keys.getOrDefault(key, Set.of()));
        }

        public void markDisconnected(SocketConnection c) {
            var set = client2keys.get(c);
            if (set != null) {
                for (UUID uuid : set) {
                    var lock = getLock(uuid);
                    if (lock != null) {
                        lock.disconnected = true;
                        lock.disconnectedAt = System.currentTimeMillis();
                    }
                }
            }
        }

        public Set<UUID> dropAllFor(SocketConnection c) {
            var set = client2keys.remove(c);
            if (set == null) return Set.of();
            key2client.keySet().removeAll(set);
            key2lock.keySet().removeAll(set);
            return Collections.unmodifiableSet(set);
        }

        public void unlockIfOwner(UUID key, SocketConnection connection, int token) {
            var lock = key2lock.get(key);
            if (lock == null) return;
            if (lock.token == token) {
                unlock(key);
            }
        }

        public @Nullable LockData unlock(UUID key) {
            var lock = key2lock.remove(key);
            if (lock == null) return null;
            key2client.remove(key);
            var v = client2keys.get(lock.owner);
            if (v != null) v.remove(key);
            return lock;
        }

        public @Nullable LockData tryLock(UUID key, SocketConnection c) {
            var old = key2lock.get(key);
            if (old != null) {
                if (old.disconnected && old.disconnectedAt + 2_000 <= System.currentTimeMillis()) {
                    unlock(key);
                    return tryLock(key, c);
                }
                // if (old.owner == c) {
                //     old.lastConfirm = System.currentTimeMillis();
                //     old.token = counter.incrementAndGet();
                //     return old;
                // }
                return null;
            }
            LockData lock = new LockData(key, c, counter.incrementAndGet());
            lock.lastConfirm = System.currentTimeMillis();
            key2lock.put(key, lock);
            key2client.put(key, c);
            client2keys.computeIfAbsent(c, k -> new HashSet<>()).add(key);
            return lock;
        }

    }

    private String arrayToBase64(byte[] arr) {
        return arr == null ? "null" : Base64.getEncoder().encodeToString(arr);
    }

    private static class MailBox {
        private static final Logger log = LoggerFactory.getLogger(MailBox.class);
        private int mailIds = 0;
        private final BatchedMailbox mailbox;
        private final Cache<UUID, Queue<MailboxRepository.Mail>> loaded_mails;

        private MailBox(BatchedMailbox mailbox) {
            this.mailbox = mailbox;
            try {
                mailIds = mailbox.getMaxId();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            loaded_mails = Caffeine.newBuilder()
                    .maximumSize(2048)
                    .expireAfterAccess(Duration.ofMinutes(30))
                    .build()
            ;
        }

        public int nextMailId() {
            return mailIds++;
        }

        public MailboxRepository.Mail peekNextMail(UUID uuid) {
            return getMailQueue(uuid).peek();
        }

        public Queue<MailboxRepository.Mail> getMailQueue(UUID uuid) {
            return loaded_mails.get(uuid, k -> {
                try {
                    var list = mailbox.loadAll(uuid);
                    PriorityQueue<MailboxRepository.Mail> queue = new PriorityQueue<>(Comparator.comparingInt(MailboxRepository.Mail::id));
                    queue.addAll(list);
                    return queue;
                } catch (SQLException e) {
                    log.error("Failed to load mails for {}", uuid);
                    return new PriorityQueue<>();
                }
            });
        }

        public void removeMail(MailboxRepository.Mail mail) {
            mailbox.removeMail(mail);
            var v = loaded_mails.getIfPresent(mail.owner());
            if (v != null) {
                v.remove(mail);
            }
        }

        public void addMail(MailboxRepository.Mail mail) {
            getMailQueue(mail.owner()).offer(mail);
            mailbox.addMail(mail);
        }

        public void close() {
            BSUtils.safe(mailbox::close);
        }
    }
}
