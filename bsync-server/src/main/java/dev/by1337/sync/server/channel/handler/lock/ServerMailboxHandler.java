package dev.by1337.sync.server.channel.handler.lock;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import dev.by1337.sync.common.channel.GetPostChannelHandler;
import dev.by1337.sync.common.callback.ResponseFuture;
import dev.by1337.sync.common.channel.pipeline.ChannelRuntime;
import dev.by1337.sync.common.channel.pipeline.Connection;
import dev.by1337.sync.common.channel.pipeline.Pipeline;
import dev.by1337.sync.common.packet.impl.c2s.C2SMailResponsePacket;
import dev.by1337.sync.common.packet.impl.c2s.C2SGiveMailUidsRequest;
import dev.by1337.sync.common.packet.impl.a2a.A2ALongResponse;
import dev.by1337.sync.common.packet.impl.a2a.A2AFlagResponse;
import dev.by1337.sync.common.packet.impl.c2s.C2SPollAllMailsPacket;
import dev.by1337.sync.common.packet.impl.c2s.C2SPushMailPacket;
import dev.by1337.sync.common.packet.impl.s2c.S2CMailAcceptPacket;
import dev.by1337.sync.common.util.BSUtils;
import dev.by1337.sync.common.work.EventLoopWorker;
import dev.by1337.sync.server.DedicatedServer;
import dev.by1337.sync.server.channel.ServerChannelRuntime;
import dev.by1337.sync.server.database.table.BatchedMailbox;
import dev.by1337.sync.server.database.table.MailboxRepository;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public class ServerMailboxHandler extends GetPostChannelHandler implements MailboxHandler {
    private static final AtomicLong MAIL_UIDS = new AtomicLong();

    private Logger log = DEFAULT_LOGGER;
    private ServerLockerHandler locks;
    private EventLoopWorker eventLoop;
    private Pipeline pipeline;
    private boolean closing = false;
    private MailBox mailBox;
    private ServerChannelRuntime serverChannel;
    private LongSet diffsA = new LongOpenHashSet();
    private LongSet diffsB = new LongOpenHashSet();
    private final long deliveryIdBase = ThreadLocalRandom.current().nextLong();

    public ServerMailboxHandler() {
        registerGet(C2SGiveMailUidsRequest.class, msg -> new ResponseFuture<>(
                new A2ALongResponse(MAIL_UIDS.getAndAdd(C2SGiveMailUidsRequest.RANGE_SIZE))));
        registerPost(C2SPollAllMailsPacket.class, (ctx, msg) -> {
            if (locks.isOwner(msg.key(), ctx.connection().transport(), msg.token())) {
                sendMail(ctx.connection(), msg.key());
            }
        });
        registerGet(C2SPushMailPacket.class, msg -> {
            if (diffsA.contains(msg.uid()) || diffsB.contains(msg.uid()))
                return new ResponseFuture<>(new A2AFlagResponse(true));
            UUID key = msg.key();
            var json = msg.json();
            MailboxRepository.Mail mail = new MailboxRepository.Mail(mailBox.nextMailId(), key, json);
            mailBox.addMail(mail);
            diffsA.add(msg.uid());
            var lock = locks.getLock(key);
            if (lock != null) BSUtils.safe(() -> {
                var connection = serverChannel.lookup(lock.owner);
                if (connection != null) sendMail(connection, key);
            });
            return new ResponseFuture<>(new A2AFlagResponse(true));
        });
    }

    @Override
    public void pushMail(UUID key, String json) {
        new C2SPushMailPacket(key, json, MAIL_UIDS.getAndIncrement()).request(pipeline);
    }

    @Override
    public void init(ChannelRuntime r) {
        if (!(r instanceof ServerChannelRuntime runtime))
            throw new IllegalArgumentException("runtime must be a ServerChannelRuntime");
        serverChannel = runtime;
        log = r.logger();
        pipeline = runtime.pipeline();
        eventLoop = r.eventLoop();
        locks = pipeline.get(ServerLockerHandler.class);
        mailBox = new MailBox(new BatchedMailbox(
                new MailboxRepository(
                        runtime.server().database().dataSource(),
                        runtime.channel().id() + "_mailbox_repository"
                ),
                DedicatedServer.IO_WORKERS.getNext()
        ));
        eventLoop.repeat(this::rotateDiffs, 60_000 * 5, () -> closing);
    }

    private void rotateDiffs() {
        var v = diffsB;
        diffsB = diffsA;
        diffsA = v;
        v.clear();
    }

    @Override
    public void close() {
        closing = true;
        BSUtils.safe(mailBox::close);
    }

    private void sendMail(Connection connection, UUID key) {
        eventLoop.assertThread();
        var lock = locks.getLock(key);
        if (lock == null || lock.owner != connection.transport()) return;
        if (lock.isMailProcess) return;
        var mail = mailBox.peekNextMail(key);
        if (mail == null) return;
        lock.isMailProcess = true;
        int deliveryToken = lock.token;
        new S2CMailAcceptPacket(key, mail.payload(), deliveryToken, deliveryIdBase + mail.id()).request(pipeline, connection)
                .then((result) -> {
                    lock.isMailProcess = false;
                    eventLoop.assertThread();
                    if (result instanceof C2SMailResponsePacket response) {
                        // The request was sent to this lock owner. A valid acceptance remains
                        // valid after unlock/relock; otherwise an already processed mail survives.
                        if (response.token() != deliveryToken) {
                            log.error("Клиент {} подтвердил mail с неверным токеном! {} {}", connection.transport(), mail, response);
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
            return ++mailIds;
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
