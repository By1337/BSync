package dev.by1337.sync.client.channel;

import dev.by1337.sync.client.network.Connection;
import dev.by1337.sync.common.callback.PacketCallback;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.impl.c2s.*;
import dev.by1337.sync.common.packet.impl.s2c.*;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;

public abstract class DataChannel extends AbstractChannel {

    private final Logger log;
    private final Set<UUID> locks = new HashSet<>();

    protected DataChannel(Connection connection, String id) {
        super(connection, id);
        log = LoggerFactory.getLogger(id + "|DataChannel");
    }

    protected abstract byte[] forceUnlockNow(UUID key);

    @Override
    public void onReceive(Packet packet) {
        assertThread();
        if (packet instanceof S2CForceUnlockPacket unlock) {
            if (locks.remove(unlock.key)) {
                byte[] arr = forceUnlockNow(unlock.key);
                log.error("Force unlocked by server {}! DATA LOST {}", unlock.key, Base64.getEncoder().encodeToString(arr));
            }
        }
    }

    public void lockAndLoadData(UUID key, BiConsumer<LockStatus, byte @Nullable []> callback) {
        request(new C2SLockAndGetBlobRequestPacket(key), packet -> {
            if (packet instanceof S2CLockStatusAndBlobPacket status) {
                if (status.isAccepted()) {
                    locks.add(key);
                }
                callback.accept(status.isAccepted() ? LockStatus.SUCCESS : LockStatus.FAILURE, status.blob);
                if (status.isAccepted()) {
                    send(new C2SPollAllMailsPacket(key));
                }
            } else {
                log.error("Failed to get lock for {} Response {}", key, packet);
                callback.accept(LockStatus.FAILURE, null);
            }
        }, 15_000);
    }

    protected abstract void onMailAccept(UUID key, String json);

    @Override
    protected void onRequest(Packet packet, PacketCallback consumer) {
        assertThread();
        if (packet instanceof S2CMailAcceptPacket mail) {
            if (!locks.contains(mail.key)) {
                consumer.accept(C2SMailResponsePacket.reject());
                return;
            }
            consumer.accept(C2SMailResponsePacket.accepted());
            try {
                onMailAccept(mail.key, mail.json);
            } catch (Exception e) {
                log.error("Failed to accept mail {}", mail, e);
            }
        } else if (packet instanceof S2CLockStatusRequestPacket status) {
            if (locks.contains(status.key)) {
                consumer.accept(C2SLockStatusResponsePacket.locked());
            } else {
                consumer.accept(C2SLockStatusResponsePacket.free());
            }
        }
    }

    public void pushMail(UUID key, String json) {
        execute(() -> {
            if (!locks.contains(key)) {
                send(new C2SPushMailPacket(key, json));
            } else {
                try {
                    onMailAccept(key, json);
                } catch (Exception e) {
                    log.error("Failed to accept mail {} {}", key, json, e);
                }
            }
        });
    }
    public void unlock(UUID key, byte[] blob){
        execute(() -> {
            if (locks.remove(key)) {
                send(new C2SUnlockAndFlushBlobPacket(key, blob));
            }else {
                log.error("Failed to unlock {} DATA LOST {}", key, Base64.getEncoder().encodeToString(blob));
            }
        });
    }

    @Override
    public void onRegister() {
    }

    @Override
    public void onUnregister() {
    }

    @Override
    public void onChannelActive() {
        assertThread();
        for (UUID lock : locks) {
            request(new C2SRelockRequestPacket(lock), packet -> {
                if (!(packet instanceof S2CLockStatusPacket status) || status.isRejected()) {
                    locks.remove(lock);
                    byte[] arr = forceUnlockNow(lock);
                    log.error("Failed to reget lock for {}! DATA LOST {} {}", lock, Base64.getEncoder().encodeToString(arr), packet);
                }
            }, 15_000);
        }
    }

    @Override
    public void onClose() {
        super.onClose();
        for (UUID lock : locks) {
            try {
                send(new C2SUnlockAndFlushBlobPacket(lock, forceUnlockNow(lock)));
            } catch (Exception e) {
                log.error("Failed to flush data for {}", lock, e);
            }
        }
        locks.clear();
    }

    public enum LockStatus {
        SUCCESS,
        FAILURE
    }
}
