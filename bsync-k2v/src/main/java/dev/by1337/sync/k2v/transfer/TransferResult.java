package dev.by1337.sync.k2v.transfer;

public record TransferResult(Status status, String detail) {
    public static TransferResult success() {
        return new TransferResult(Status.SUCCESS, "");
    }

    public enum Status {
        SUCCESS, LOCKED, FAILED, UNSUPPORTED
    }
}
