package dev.by1337.sync;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;

public interface DataManager<T> {
    @NotNull T read(byte @Nullable [] data);

    byte @NotNull [] write(@NotNull T data);

    void acceptMail(@NotNull T data, @NotNull String mail);

    void forceUnlock(UUID key);
}
