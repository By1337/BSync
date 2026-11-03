package dev.by1337.sync.common.transfer;

import dev.by1337.sync.common.mail.Mail;
import java.util.Objects;
import java.util.UUID;

public record TransferEntry(Kind kind, UUID key, byte[] payload, UUID messageId) {
    public static final int MAX_DATA_BYTES = (16 << 20) - 128;
    public static final int PAGE_BYTES = 4 << 20;
    public static final int PAGE_ROWS = 256;

    public TransferEntry {
        Objects.requireNonNull(kind);
        Objects.requireNonNull(key);
        Objects.requireNonNull(payload);
        int max = kind == Kind.DATA ? MAX_DATA_BYTES : Mail.MAX_PAYLOAD_BYTES;
        if (payload.length > max) throw new IllegalArgumentException("Oversized " + kind + " record");
        if (kind == Kind.MAIL) Objects.requireNonNull(messageId);
    }

    public enum Kind {
        DATA, MAIL
    }
}
