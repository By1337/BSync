package dev.by1337.sync.common.mail;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

public record Mail(UUID id, UUID owner, String payload) {
    public static final int MAX_PAYLOAD_BYTES = 1_048_576;

    public Mail {
        Objects.requireNonNull(id);
        Objects.requireNonNull(owner);
        Objects.requireNonNull(payload);
        if (payload.getBytes(StandardCharsets.UTF_8).length > MAX_PAYLOAD_BYTES)
            throw new IllegalArgumentException("Mail exceeds 1 MiB");
    }
}
