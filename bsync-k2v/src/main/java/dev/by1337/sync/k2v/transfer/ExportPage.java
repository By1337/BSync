package dev.by1337.sync.k2v.transfer;

import java.util.List;
import java.util.UUID;

/** Storage export page, independent of the BSync wire protocol. */
public record ExportPage(List<TransferEntry> entries, UUID afterKey, long afterSequence, boolean done, String error) {
    public static ExportPage empty() {
        return new ExportPage(List.of(), null, 0, true, "");
    }

    public static ExportPage failed(String error) {
        return new ExportPage(List.of(), null, 0, true, error);
    }
}
