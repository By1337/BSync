package dev.by1337.sync.client.channel.handler;

import dev.by1337.sync.client.channel.ClientChannelRuntime;
import dev.by1337.sync.client.channel.status.ChannelActiveMessage;
import dev.by1337.sync.client.channel.status.ChannelInactiveMessage;
import dev.by1337.sync.common.channel.ChannelMessage;
import dev.by1337.sync.common.channel.handler.IncomingRequest;
import dev.by1337.sync.common.channel.handler.RequestsHandler;
import dev.by1337.sync.common.channel.pipeline.*;
import dev.by1337.sync.common.packet.impl.c2s.*;
import dev.by1337.sync.common.packet.impl.s2c.*;
import dev.by1337.sync.common.work.EventLoopWorker;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;

import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.BiConsumer;

public abstract class ClientLocksHandler implements ChannelHandler {
    private Logger log = DEFAULT_LOGGER;
    private EventLoopWorker eventLoop;
    private final Set<UUID> locks = new ConcurrentSkipListSet<>();
    private Connection freedom;
    private Pipeline pipeline;

    @Override
    public final void init(ChannelRuntime runtime) {
        if (!(runtime instanceof ClientChannelRuntime ccr)) throw new IllegalArgumentException("Invalid runtime type");
        if (this.eventLoop != null) {
            throw new IllegalStateException("Duplicate handler add!");
        }
        freedom = ccr.freedom();
        this.eventLoop = runtime.eventLoop();
        this.log = runtime.logger();
        pipeline = runtime.pipeline();
    }

    public Logger getLogger() {
        return log;
    }

    public boolean isLocked(UUID uuid) {
        return locks.contains(uuid);
    }

    protected abstract byte[] forceUnlockNow(UUID key);

    protected abstract void onMailAccept(UUID key, String json);

    @Override
    public final void handle(ChannelContext ctx, ChannelMessage msg) {
        if (msg instanceof S2CForceUnlockPacket unlock) {
            if (locks.remove(unlock.key)) {
                byte[] arr = forceUnlockNow(unlock.key);
                log.error("Force unlocked by server {}! DATA LOST {}", unlock.key, Base64.getEncoder().encodeToString(arr));
            }
        } else if (msg instanceof IncomingRequest r) {
            if (r.payload() instanceof S2CMailAcceptPacket mail) {
                if (!locks.contains(mail.key)) {
                    r.out().accept(C2SMailResponsePacket.reject());
                } else {
                    r.out().accept(C2SMailResponsePacket.accepted());
                    try {
                        onMailAccept(mail.key, mail.json);
                    } catch (Exception e) {
                        log.error("Failed to accept mail {}", mail, e);
                    }
                }
            } else if (r.payload() instanceof S2CLockStatusRequestPacket status) {
                if (locks.contains(status.key)) {
                    r.out().accept(C2SLockStatusResponsePacket.locked());
                } else {
                    r.out().accept(C2SLockStatusResponsePacket.free());
                }
            }
        } else {
            if (msg instanceof ChannelActiveMessage) {
                for (UUID lock : List.copyOf(locks)) {
                    pipeline.handle(RequestsHandler.request(new C2SRelockRequestPacket(lock), (packet, con) -> {
                        if (!(packet instanceof S2CLockStatusPacket status) || status.isRejected()) {
                            locks.remove(lock);
                            byte[] arr = forceUnlockNow(lock);
                            log.error("Failed to reget lock for {}! DATA LOST {} {}", lock, Base64.getEncoder().encodeToString(arr), packet);
                        }
                    }, 15_000), freedom);
                }
            } else if (msg instanceof ChannelInactiveMessage) {
                log.warn("Connection lost maybe data lost!");
            }
            ctx.fire(msg);
        }
    }

    @Override
    public void close() {
        eventLoop.assertThread();
        for (UUID lock : List.copyOf(locks)) {
            try {
                freedom.write(new C2SUnlockAndFlushBlobPacket(lock, forceUnlockNow(lock)));
            } catch (Exception e) {
                log.error("Failed to flush data for {}", lock, e);
            }
        }
    }

    public void pushMail(UUID key, String json) {
        eventLoop.execute(() -> {
            if (!locks.contains(key)) {
                freedom.write(new C2SPushMailPacket(key, json));
            } else {
                try {
                    onMailAccept(key, json);
                } catch (Exception e) {
                    log.error("Failed to accept mail {} {}", key, json, e);
                }
            }
        });
    }

    public void unlock(UUID key, byte @Nullable [] blob) {
        eventLoop.execute(() -> {
            if (locks.remove(key)) {
                freedom.write(new C2SUnlockAndFlushBlobPacket(key, blob));
            } else {
                log.error("Failed to unlock {} DATA LOST {}", key, Base64.getEncoder().encodeToString(blob));
            }
        });
    }

    public void lockAndLoadData(UUID key, BiConsumer<LockStatus, byte @Nullable []> callback) {
        pipeline.handle(RequestsHandler.request(
                new C2SLockAndGetBlobRequestPacket(key),
                (packet, con) -> {
                    if (packet instanceof S2CLockStatusAndBlobPacket status) {
                        if (status.isAccepted()) {
                            locks.add(key);
                        }
                        callback.accept(status.isAccepted() ? LockStatus.SUCCESS : LockStatus.FAILURE, status.blob);
                        if (status.isAccepted()) {
                            con.write(new C2SPollAllMailsPacket(key));
                        }
                    } else {
                        log.error("Failed to get lock for {} Response {}", key, packet);
                        callback.accept(LockStatus.FAILURE, null);
                    }
                },
                15_000
        ), freedom);
    }

    public enum LockStatus {
        SUCCESS,
        FAILURE
    }
}
