package dev.by1337.sync.server.channel.handler.lock;

import dev.by1337.sync.common.callback.ResponseFuture;
import dev.by1337.sync.common.channel.GetPostChannelHandler;
import dev.by1337.sync.common.channel.pipeline.ChannelRuntime;
import dev.by1337.sync.common.channel.pipeline.Pipeline;
import dev.by1337.sync.common.packet.impl.c2s.C2SFlushBlobPacket;
import dev.by1337.sync.common.packet.impl.c2s.C2SLoadSnapshotPacket;
import dev.by1337.sync.common.packet.impl.s2c.S2CSnapshotPacket;
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
    byte[] loadBlob(UUID key){
        return BSUtils.safeOptional(() -> blobRepository.get(key));
    }

    @Override
    public void init(ChannelRuntime r) {
        if (!(r instanceof ServerChannelRuntime runtime))
            throw new IllegalArgumentException("runtime must be a ServerChannelRuntime");
        EventLoopWorker eventLoop = runtime.eventLoop();
        Logger log = runtime.logger();
        Pipeline pipeline = runtime.pipeline();
        DedicatedServer server = runtime.server();
        locks = pipeline.get(ServerLockerHandler.class);
        blobRepository = new BatchedK2VCache<>(
                new dev.by1337.sync.bd.repo.UUID2MediumBLOBRepository(server.database().dataSource(), runtime.channel().id() + "_blob_repository"),
                DedicatedServer.IO_WORKERS.getNext(),
                b -> b
                        .maximumWeight(1024 * 1024 * 1024)
                        .weigher((k, v) -> v.length + 16)
        );
    }

    @Override
    public void close() {
        closing = true;
        BSUtils.safe(blobRepository::close);
    }
}
