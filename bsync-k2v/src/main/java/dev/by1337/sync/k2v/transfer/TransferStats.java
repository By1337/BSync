package dev.by1337.sync.k2v.transfer;

public record TransferStats(long data, long mails, long locked, long failed) {
    public boolean successful() {
        return locked == 0 && failed == 0;
    }
}
