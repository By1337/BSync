package dev.by1337.sync.k2v.transfer;

import java.util.concurrent.CompletableFuture;


/** One source per shard, created anew for each export. */
public interface ExportSource extends AutoCloseable {
    CompletableFuture<ExportPage> next(TransferEntry.Kind kind);

    @Override
    default void close() throws Exception {
    }
}
