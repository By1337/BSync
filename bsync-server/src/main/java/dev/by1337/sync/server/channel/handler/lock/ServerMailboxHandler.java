package dev.by1337.sync.server.channel.handler.lock;

import dev.by1337.sync.common.callback.ResponseFuture;
import dev.by1337.sync.common.channel.GetPostChannelHandler;
import dev.by1337.sync.common.channel.pipeline.*;
import dev.by1337.sync.common.mail.Mail;
import dev.by1337.sync.common.mail.MailJournal;
import dev.by1337.sync.common.packet.impl.a2a.A2AFlagResponse;
import dev.by1337.sync.common.packet.impl.c2s.*;
import dev.by1337.sync.common.packet.impl.s2c.S2CExportPagePacket;
import dev.by1337.sync.common.packet.impl.s2c.S2CMailAcceptPacket;
import dev.by1337.sync.common.transfer.TransferEntry;
import dev.by1337.sync.common.work.EventLoopWorker;
import dev.by1337.sync.server.DedicatedServer;
import dev.by1337.sync.server.channel.ServerChannelRuntime;
import dev.by1337.sync.server.database.table.ReliableMailboxRepository;
import org.slf4j.Logger;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class ServerMailboxHandler extends GetPostChannelHandler implements MailboxHandler {
    private static final ScheduledExecutorService DISPATCH_RETRY =
            Executors.newSingleThreadScheduledExecutor(task -> {
                Thread thread = new Thread(task, "bsync-mailbox-dispatch");
                thread.setDaemon(true);
                return thread;
            });
    private Logger log = DEFAULT_LOGGER;
    private ServerLockerHandler locks;
    private EventLoopWorker eventLoop;
    private EventLoopWorker io;
    private Pipeline pipeline;
    private volatile boolean closing;
    private ReliableMailboxRepository mailbox;
    private MailJournal localOutbox;
    private ServerChannelRuntime serverChannel;
    private final Set<UUID> processing = new HashSet<>();
    private final Set<UUID> localInFlight = new HashSet<>();

    public ServerMailboxHandler() {
        registerPost(C2SPollAllMailsPacket.class, (ctx, packet) -> {
            if (locks.isOwner(packet.key(), ctx.connection().transport(), packet.token())) deliver(packet.key());
        });
        registerGet(C2SPushMailPacket.class, packet -> {
            var response = new ResponseFuture<A2AFlagResponse>();
            Mail mail = new Mail(packet.uid(), packet.key(), packet.json());
            database(() -> mailbox.put(mail), accepted -> {
                if (!Boolean.TRUE.equals(accepted))
                    log.error("Mail admission failed id={} owner={}; producer must retain its outbox record", mail.id(), mail.owner());
                response.complete(new A2AFlagResponse(Boolean.TRUE.equals(accepted)));
                if (Boolean.TRUE.equals(accepted)) deliver(mail.owner());
            });
            return response;
        });
    }

    @Override
    public void init(ChannelRuntime r) {
        if (!(r instanceof ServerChannelRuntime runtime)) throw new IllegalArgumentException("Expected server runtime");
        serverChannel = runtime;
        log = r.logger();
        pipeline = r.pipeline();
        eventLoop = r.eventLoop();
        io = DedicatedServer.IO_WORKERS.getNext();
        locks = pipeline.get(ServerLockerHandler.class);
        String identity = UUID.nameUUIDFromBytes(runtime.channel().id().getBytes(StandardCharsets.UTF_8)).toString().replace("-", "");
        try {
            mailbox = new ReliableMailboxRepository(runtime.server().database().dataSource(), "mailbox_" + identity);
            mailbox.migrateLegacy(runtime.channel().id() + "_mailbox_repository");
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot initialize durable mailbox", e);
        }
        localOutbox = new MailJournal(Path.of("bsync", "server-mail-outbox", identity));
        log.info("Durable mailbox initialized table={} outbox={}", "mailbox_" + identity,
                Path.of("bsync", "server-mail-outbox", identity));
        eventLoop.repeat(this::retry, 1_000, () -> closing);
    }

    @Override
    public void pushMail(UUID key, String json) {
        if (closing || localOutbox == null) throw new IllegalStateException("Mailbox not available");
        localOutbox.append(key, json);
        eventLoop.execute(this::retry);
    }

    ResponseFuture<S2CExportPagePacket> exportPage(C2SExportPagePacket request) {
        var response = new ResponseFuture<S2CExportPagePacket>();
        database(() -> mailbox.exportPage(request.afterSequence(), request.shard()::includes), page -> {
            if (page == null) {
                response.complete(S2CExportPagePacket.failed("Mailbox SQL export failed"));
                return;
            }
            var entries = page.entries().stream().map(mail -> new TransferEntry(
                    TransferEntry.Kind.MAIL, mail.owner(),
                    mail.payload().getBytes(StandardCharsets.UTF_8), mail.id())).toList();
            response.complete(new S2CExportPagePacket(entries, null,
                    page.afterSequence(), page.done(), ""));
        });
        return response;
    }

    private void retry() {
        if (closing) return;
        for (UUID key : locks.lockedKeys()) deliver(key);
        for (Mail mail : localOutbox.pending()) {
            if (localInFlight.size() >= 32) break;
            if (!localInFlight.add(mail.id())) continue;
            database(() -> mailbox.put(mail), accepted -> {
                localInFlight.remove(mail.id());
                if (Boolean.TRUE.equals(accepted)) {
                    try {
                        localOutbox.acknowledge(mail.id());
                    } catch (RuntimeException e) {
                        log.error("Cannot acknowledge local mail outbox id={}", mail.id(), e);
                    }
                    deliver(mail.owner());
                } else log.error("Local mail admission failed id={} owner={}; retained on disk", mail.id(), mail.owner());
            });
        }
    }

    private void deliver(UUID key) {
        if (closing) return;
        var lock = locks.getLock(key);
        if (lock == null || lock.importing || !processing.add(key)) return;
        Connection connection = serverChannel.lookup(lock.owner);
        if (connection == null) {
            processing.remove(key);
            return;
        }
        int token = lock.token;
        database(() -> mailbox.next(key), pending -> {
            var current = locks.getLock(key);
            if (pending == null || current != lock || pending.retryAt() > System.currentTimeMillis()) {
                processing.remove(key);
                return;
            }
            Mail mail = pending.mail();
            new S2CMailAcceptPacket(key, mail.payload(), token, mail.id()).request(pipeline, connection)
                    .then(response -> {
                        if (response != null && response.token() == token && response.isAccepted()) {
                            // Acceptance is for this delivery, even if the player has since moved.
                            database(() -> {
                                mailbox.acknowledge(mail.id());
                                return true;
                            }, committed -> {
                                processing.remove(key);
                                if (Boolean.TRUE.equals(committed)) deliver(key);
                            });
                        } else {
                            long delay = Math.min(60_000L, 1_000L << (int) Math.min(pending.attempts() + 1, 6));
                            String error = response == null ? "No delivery ACK" : "Rejected delivery: " + response;
                            log.error("Mail delivery failed id={} owner={} attempts={} retryInMs={} reason={}",
                                    mail.id(), key, pending.attempts() + 1, delay, error);
                            database(() -> {
                                mailbox.failed(mail.id(), error, System.currentTimeMillis() + delay);
                                return true;
                            }, ignored -> processing.remove(key));
                        }
                    });
        });
    }

    private <T> void database(SqlTask<T> task, Consumer<T> done) {
        dispatch(io, () -> {
            T result = null;
            try {
                result = task.run();
            } catch (Exception e) {
                log.error("Mailbox database operation failed; records remain pending", e);
            }
            T value = result;
            dispatch(eventLoop, () -> {
                if (!closing) done.accept(value);
            });
        });
    }

    private void dispatch(EventLoopWorker worker, Runnable task) {
        if (closing) return;
        if (!worker.tryExecute(task)) {
            log.warn("Mailbox worker {} is saturated; rescheduling instead of dropping work", worker.name());
            DISPATCH_RETRY.schedule(() -> dispatch(worker, task), 100, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void close() {
        closing = true;
        if (localOutbox != null) localOutbox.close();
        // All admissions were committed before ACK. Unacknowledged deliveries stay in SQL.
    }

    @FunctionalInterface
    private interface SqlTask<T> {
        T run() throws Exception;
    }
}
