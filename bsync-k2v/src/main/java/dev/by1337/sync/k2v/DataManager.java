package dev.by1337.sync.k2v;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;

public interface DataManager<T> {
    @NotNull T read(byte @Nullable [] data, @NotNull UUID key);

    byte @NotNull [] write(@NotNull T data, @NotNull UUID key);

    default void acceptMail(@NotNull T data, @NotNull String mail, @NotNull UUID key) {
    }

    default void forceUnlock(UUID key) {
    }
}
