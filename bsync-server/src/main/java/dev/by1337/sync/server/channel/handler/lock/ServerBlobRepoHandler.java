package dev.by1337.sync.server.channel.handler.lock;

import dev.by1337.sync.bd.repo.UUID2MediumBLOBRepository;
import dev.by1337.sync.common.callback.ResponseFuture;
import dev.by1337.sync.common.channel.GetPostChannelHandler;
import dev.by1337.sync.common.channel.pipeline.ChannelRuntime;
import dev.by1337.sync.common.channel.pipeline.Pipeline;
import dev.by1337.sync.common.packet.impl.c2s.C2SExportPagePacket;
import dev.by1337.sync.common.packet.impl.c2s.C2SFlushBlobPacket;
import dev.by1337.sync.common.packet.impl.c2s.C2SImportDataPacket;
import dev.by1337.sync.common.packet.impl.c2s.C2SLoadSnapshotPacket;
import dev.by1337.sync.common.packet.impl.s2c.S2CExportPagePacket;
import dev.by1337.sync.common.packet.impl.s2c.S2CImportResultPacket;
import dev.by1337.sync.common.packet.impl.s2c.S2CSnapshotPacket;
import dev.by1337.sync.common.transfer.TransferEntry;
import dev.by1337.sync.common.transfer.TransferResult;
import dev.by1337.sync.common.util.BSUtils;
import dev.by1337.sync.common.work.EventLoopWorker;
import dev.by1337.sync.server.DedicatedServer;
import dev.by1337.sync.server.channel.ServerChannelRuntime;
import dev.by1337.sync.server.database.table.BatchedK2VCache;
import org.slf4j.Logger;

import java.util.UUID;

public class ServerBlobRepoHandler extends GetPostChannelHandler {

    private boolean closing = false;
    private ServerLockerHandler locks;
    private BatchedK2VCache<UUID, byte[]> blobRepository;
    private UUID2MediumBLOBRepository persistent;
    private EventLoopWorker io;
    private EventLoopWorker eventLoop;
    private Logger log;

    public ServerBlobRepoHandler() {
        registerGet(C2SLoadSnapshotPacket.class, r -> new ResponseFuture<>(new S2CSnapshotPacket(loadBlob(r.key()))));
        registerPost(C2SFlushBlobPacket.class, (ctx, flush) -> {
            var lock = locks.getLock(flush.key());
            if (lock != null && lock.isOwner(flush.token(), ctx.connection().transport()) && flush.version() > lock.snapshotVersion) {
                lock.snapshotVersion = flush.version();
                var blob = flush.blob();
                if (blob != null) {
                    BSUtils.safe(() -> blobRepository.put(lock.key, flush.blob()));
                }
            }
        });
    }

    byte[] loadBlob(UUID key) {
        return BSUtils.safeOptional(() -> blobRepository.get(key));
    }

    @Override
    public void init(ChannelRuntime r) {
        if (!(r instanceof ServerChannelRuntime runtime))
            throw new IllegalArgumentException("runtime must be a ServerChannelRuntime");
        eventLoop = runtime.eventLoop();
        log = runtime.logger();
        io = DedicatedServer.IO_WORKERS.getNext();
        Pipeline pipeline = runtime.pipeline();
        DedicatedServer server = runtime.server();
        locks = pipeline.get(ServerLockerHandler.class);
        persistent = new UUID2MediumBLOBRepository(server.database().dataSource(), runtime.channel().id() + "_blob_repository");
        blobRepository = new BatchedK2VCache<>(
                persistent,
                io,
                b -> b
                        .maximumWeight(1024 * 1024 * 1024)
                        .weigher((k, v) -> v.length + 16)
        );
    }

    ResponseFuture<S2CExportPagePacket> exportPage(
        C2SExportPagePacket request) {
        var result = new ResponseFuture<S2CExportPagePacket>();
        io.executeReliably(() -> {
            S2CExportPagePacket response;
            try {
                blobRepository.flushPending();
                var page = persistent.exportPage(request.afterKey(), 256, TransferEntry.PAGE_BYTES, request.shard()::includes);
                var entries = page.entries().stream().map(pair -> new TransferEntry(
                        TransferEntry.Kind.DATA, pair.key, pair.value, null)).toList();
                response = new S2CExportPagePacket(entries, page.afterKey(), 0, page.done(), "");
            } catch (Exception e) {
                log.error("Snapshot SQL export failed", e);
                response = S2CExportPagePacket.failed("Snapshot SQL export failed");
            }
            var complete = response;
            eventLoop.executeReliably(() -> result.complete(complete));
        });
        return result;
    }

    ResponseFuture<S2CImportResultPacket> importData(
        C2SImportDataPacket request,
        dev.by1337.sync.common.channel.pipeline.Connection connection) {
        var result = new ResponseFuture<S2CImportResultPacket>();
        var lock = locks.reserveImport(request.key(), connection.transport());
        if (lock == null) {
            log.warn("Snapshot import skipped: player {} is locked", request.key());
            result.complete(new S2CImportResultPacket(
                    TransferResult.Status.LOCKED, "Player is locked"));
            return result;
        }
        io.executeReliably(() -> {
            boolean success;
            try {
                if (closing) throw new IllegalStateException("Storage is closed");
                blobRepository.putCommitted(request.key(), request.data());
                success = true;
            } catch (Exception e) {
                log.error("Snapshot import failed for {}", request.key(), e);
                success = false;
            }
            boolean committed = success;
            eventLoop.executeReliably(() -> {
                locks.releaseImport(lock);
                result.complete(new S2CImportResultPacket(
                        committed ? TransferResult.Status.SUCCESS
                                : TransferResult.Status.FAILED,
                        committed ? "" : "Snapshot SQL write failed; see server log"));
            });
        });
        return result;
    }

    @Override
    public void close() {
        closing = true;
        BSUtils.safe(blobRepository::close);
    }
}
