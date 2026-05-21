package dev.by1337.sync.server.channel;

import dev.by1337.sync.common.callback.PacketCallback;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.impl.c2s.*;
import dev.by1337.sync.common.packet.impl.s2c.S2CLockStatusAndBlobPacket;
import dev.by1337.sync.common.packet.impl.s2c.S2CLockStatusPacket;
import dev.by1337.sync.common.packet.impl.s2c.S2CMailAcceptPacket;
import dev.by1337.sync.common.work.EventLoopWorker;
import dev.by1337.sync.server.network.Connection;
import org.jetbrains.annotations.Nullable;

import java.util.*;

public class DataChannel extends AbstractChannel {

    private final Map<UUID, byte[]> database = new HashMap<>();
    private int mailIds = 0;
    private final Map<UUID, LinkedHashMap<Integer, String>> mails = new HashMap<>();

    private final LockMap lockMap = new LockMap();

    public DataChannel(EventLoopWorker worker, String id) {
        super(worker, id);
        worker.schedule(this::tick, 10_000);
    }

    private void tick() {

        worker.schedule(this::tick, 10_000);
    }

    @Override
    public void onConnect(Connection connection) {
        worker.assertThread();
    }

    @Override
    public void onDisconnect(Connection connection) {
        worker.assertThread();
        lockMap.dropAllFor(connection);
    }

    @Override
    protected void onRequest(Packet packet, PacketCallback consumer, Connection connection) {
        worker.assertThread();
        if (packet instanceof C2SLockAndGetBlobRequestPacket r) {
            var lock = lockMap.tryLock(r.key, connection);
            if (lock == null) {
                consumer.accept(new S2CLockStatusAndBlobPacket(S2CLockStatusAndBlobPacket.Status.REJECTED, null));
                return;
            }
            consumer.accept(new S2CLockStatusAndBlobPacket(S2CLockStatusAndBlobPacket.Status.ACCEPTED, database.get(r.key)));
        } else if (packet instanceof C2SRelockRequestPacket relock) {
            var lock = lockMap.tryLock(relock.key, connection);
            if (lock == null) {
                consumer.accept(S2CLockStatusPacket.reject());
            } else {
                consumer.accept(S2CLockStatusPacket.owned());
            }
        }
    }

    @Override
    public void onReceive(Packet packet, Connection connection) {
        worker.assertThread();
        if (packet instanceof C2SPollAllMailsPacket poll) {
            if (lockMap.isOwner(poll.key, connection)) {
                var mails = this.mails.get(poll.key);
                if (mails == null) return;
                new ArrayList<>(mails.keySet()).forEach(id -> doSendMail(connection, poll.key, id));
            }
        } else if (packet instanceof C2SPushMailPacket mail) {
            int id = mailIds++;
            this.mails.computeIfAbsent(mail.key, k -> new LinkedHashMap<>()).put(id, mail.json);
            var lock = lockMap.getLock(mail.key);
            if (lock != null) {
                doSendMail(lock.owner, mail.key, id);
            }
        } else if (packet instanceof C2SUnlockAndFlushBlobPacket flush) {
            var lock = lockMap.getLock(flush.key);
            if (lock == null || lock.owner != connection) {
                log.error("Клиент {} пишет в {} без блокировки! DATA LOST {}", connection, flush.key, Base64.getEncoder().encodeToString(flush.blob));
                return;
            }
            database.put(flush.key, flush.blob);
            lockMap.unlock(flush.key);
        }
    }

    private void doSendMail(Connection connection, UUID key, int id) {
        worker.assertThread();
        if (lockMap.isOwner(key, connection)) return;
        var m = this.mails.get(key);
        if (m == null) return;
        var mail = m.get(id);
        if (mail == null) return;
        request(new S2CMailAcceptPacket(key, mail), result -> {
            if (result instanceof C2SMailResponsePacket response) {
                if (response.status == C2SMailResponsePacket.Status.ACCEPTED) {
                    var mails = this.mails.get(key);
                    if (mails == null || m.remove(id) == null) {
                        log.error("Клиент {} принял не существующий mail {}", connection, mail);
                        return;
                    }
                    if (!lockMap.isOwner(key, connection)) {
                        log.error("Клиент {} принял mail {} без блокировки?", connection, mail);
                    }
                }
            } else {
                log.error("Client {} не ответил на S2CMailAcceptPacket {}", connection, result);
            }
        }, 15_000, connection);
    }

    public static class LockData {
        public final UUID key;
        public final Connection owner;
        public long lastConfirm;

        public LockData(UUID key, Connection owner) {
            this.key = key;
            this.owner = owner;
        }
    }

    public static class LockMap {
        private static final long OUTDATE_TIME_MS = 30_000;
        private final Map<Connection, Set<UUID>> client2keys = new IdentityHashMap<>(1024);
        private final Map<UUID, Connection> key2client = new HashMap<>(1024);
        private final Map<UUID, LockData> key2lock = new HashMap<>(1024);

        public @Nullable LockData getLock(UUID key) {
            return key2lock.get(key);
        }

        public boolean isOwner(UUID key, Connection connection) {
            var lock = key2lock.get(key);
            return lock != null && lock.owner == connection;
        }

        public Set<UUID> getOwnedKeys(Connection key) {
            return Collections.unmodifiableSet(client2keys.getOrDefault(key, Set.of()));
        }

        public Set<UUID> dropAllFor(Connection c) {
            var set = client2keys.remove(c);
            if (set == null) return Set.of();
            key2client.keySet().removeAll(set);
            key2lock.keySet().removeAll(set);
            return Collections.unmodifiableSet(set);
        }

        public @Nullable LockData unlock(UUID key) {
            var lock = key2lock.remove(key);
            if (lock == null) return null;
            key2client.remove(key);
            var v = client2keys.get(lock.owner);
            if (v != null) v.remove(key);
            return lock;
        }

        public @Nullable LockData tryLock(UUID key, Connection c) {
            var old = key2lock.get(key);
            if (old != null) {
                if (old.owner == c) {
                    old.lastConfirm = System.currentTimeMillis();
                    return old;
                }
                return null;
            }
            LockData lock = new LockData(key, c);
            lock.lastConfirm = System.currentTimeMillis();
            key2lock.put(key, lock);
            key2client.put(key, c);
            client2keys.computeIfAbsent(c, k -> new HashSet<>()).add(key);
            return lock;
        }

    }
}



















