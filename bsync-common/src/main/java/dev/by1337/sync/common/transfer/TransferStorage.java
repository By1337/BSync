package dev.by1337.sync.common.transfer;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;

public interface TransferStorage {
    default List<ExportSource> exportSources() {
        throw new UnsupportedOperationException("This storage does not support export");
    }

    default List<ExportSource> exportSources(ShardSelection shard) {
        if (!shard.equals(ShardSelection.ALL))
            throw new UnsupportedOperationException("This export source does not support shard filtering");
        return exportSources();
    }

    /** The list must have exactly the same order as the group's routing array. */
    static List<ExportSource> shardedExportSources(List<? extends TransferStorage> shards) {
        var sources = new ArrayList<ExportSource>();
        for (int index = 0; index < shards.size(); index++)
            sources.addAll(shards.get(index).exportSources(new ShardSelection(shards.size(), index)));
        return List.copyOf(sources);
    }

    default CompletableFuture<TransferResult> importEntry(TransferEntry entry) {
        return CompletableFuture.completedFuture(new TransferResult(TransferResult.Status.UNSUPPORTED, "Storage does not support import"));
    }
}
