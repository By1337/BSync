package dev.by1337.sync;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;

public interface DataManager<T> {
    @NotNull T read(byte @Nullable [] data, @NotNull UUID key);

    byte @NotNull [] write(@NotNull T data, @NotNull UUID key);

    void acceptMail(@NotNull T data, @NotNull String mail, @NotNull UUID key);

    void forceUnlock(UUID key);
}
