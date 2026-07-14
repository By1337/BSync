package dev.by1337.sync.common.util;

import org.jetbrains.annotations.ApiStatus;

/**
 * @hidden
 */
@ApiStatus.Internal
public final class SneakyThrow {

    public static void sneaky(final Throwable exception) {
        SneakyThrow.throwSneaky(exception);
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void throwSneaky(final Throwable exception) throws T {
        throw (T) exception;
    }

}
