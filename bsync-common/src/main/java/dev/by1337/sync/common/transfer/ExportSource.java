package dev.by1337.sync.common.transfer;

import java.util.concurrent.CompletableFuture;
import dev.by1337.sync.common.packet.impl.s2c.S2CExportPagePacket;

/** One source per shard, created anew for each export. */
public interface ExportSource extends AutoCloseable {
    CompletableFuture<S2CExportPagePacket> next(TransferEntry.Kind kind);

    @Override
    default void close() throws Exception {
    }
}
