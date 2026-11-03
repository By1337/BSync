package dev.by1337.sync.k2v.transfer;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface TransferStorage {
    default List<ExportSource> exportSources() {
        throw new UnsupportedOperationException("This storage does not support export");
    }

    default CompletableFuture<TransferResult> importEntry(TransferEntry entry) {
        return CompletableFuture.completedFuture(new TransferResult(TransferResult.Status.UNSUPPORTED, "Storage does not support import"));
    }
}
