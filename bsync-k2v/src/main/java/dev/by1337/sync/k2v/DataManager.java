package dev.by1337.sync.k2v;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;

public interface DataManager<T> {
    @NotNull T read(byte @Nullable [] data, @NotNull UUID key);

    byte @Nullable [] write(@NotNull T data, @NotNull UUID key);

    void acceptMail(@NotNull T data, @NotNull String mail, @NotNull UUID key);

    void forceUnlock(UUID key);

    interface MailBox extends DataManager<UUID> {
        @Override
        default @NotNull UUID read(byte @Nullable [] data, @NotNull UUID key) {
            return key;
        }

        @Override
        default byte @Nullable [] write(@NotNull UUID data, @NotNull UUID key) {
            return null;
        }

        @Override
        default void acceptMail(@NotNull UUID data, @NotNull String mail, @NotNull UUID key) {
            acceptMail(mail, key);
        }

        void acceptMail(@NotNull String mail, @NotNull UUID key);

        @Override
        default void forceUnlock(UUID key) {

        }
    }

    interface OnlyData<T> extends DataManager<T> {
        @Override
        default void acceptMail(@NotNull T data, @NotNull String mail, @NotNull UUID key) {
        }
    }
}
