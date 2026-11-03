package dev.by1337.sync.k2v.standalone;

import dev.by1337.sync.k2v.*;
import dev.by1337.sync.k2v.storage.*;
import dev.by1337.sync.k2v.storage.sql.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

/** Loaded in a classloader that cannot see common, client or bukkit BSync modules. */
public final class StandaloneScenario {
    public static void run(Path directory, boolean sqlite) throws Exception {
        // Verify that loading and inspecting the public factory needs no network types.
        PlayerDataRepository.class.getDeclaredMethods();
        PlayerDataStorage storage = storage(directory.resolve("source"), sqlite);
        AtomicInteger deliveries = new AtomicInteger();
        var manager = new DataManager<String>() {
            public String read(byte[] data, UUID key) {
                return data == null ? "fresh" : new String(data, StandardCharsets.UTF_8);
            }

            public byte[] write(String data, UUID key) {
                return data.getBytes(StandardCharsets.UTF_8);
            }

            public void acceptMail(String data, String mail, UUID key) {
                deliveries.incrementAndGet();
            }

            public void forceUnlock(UUID key) {
            }
        };
        UUID player = UUID.randomUUID(), offline = UUID.randomUUID();
        Path dump = directory.resolve("export.txt");
        var repository = new PlayerDataRepositoryImpl<>(storage, null, manager);
        try {
            load(repository, player);
            repository.pushSnapshot(player, "saved");
            repository.pushMail(player, "online reward");
            storage.doMailsLoad(player);
            await(() -> deliveries.get() == 1);
            repository.pushMail(offline, "offline reward");
            await(() -> {
                try {
                    return "saved".equals(repository.loadSnapshot(player).get());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            // SQLite admission runs asynchronously; inspect pending exports until admitted.
            await(() -> {
                try {
                    return storage.exportSources().get(0).next(dev.by1337.sync.k2v.transfer.TransferEntry.Kind.MAIL).get().entries().size() == 1;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            var stats = repository.exportTo(dump).get(5, TimeUnit.SECONDS);
            require(stats.data() == 1 && stats.mails() == 1, "Export lost data/mail");
        } finally {
            repository.close();
        }
        PlayerDataStorage imported = storage(directory.resolve("target"), sqlite);
        var target = new PlayerDataRepositoryImpl<>(imported, null, manager);
        try {
            target.importFrom(dump, 4).get(5, TimeUnit.SECONDS);
            require("saved".equals(target.loadSnapshot(player).get()), "Import lost snapshot");
            load(target, offline);
            imported.doMailsLoad(offline);
            await(() -> deliveries.get() == 2);
        } finally {
            target.close();
        }
    }

    private static PlayerDataStorage storage(Path directory, boolean sqlite) throws Exception {
        if (!sqlite) return new FilePlayerDataStorage(directory.toFile());
        var value = new JdbcPlayerDataStorage(SqlStorageConfig.parse("sqlite://data.db?poll=1s", directory), directory.resolve("sql"), true, true);
        try {
            var initialized = JdbcPlayerDataStorage.class.getDeclaredMethod("initialized");
            initialized.setAccessible(true);
            ((CompletableFuture<?>) initialized.invoke(value)).get(5, TimeUnit.SECONDS);
            return value;
        } catch (Exception e) {
            value.close();
            throw e;
        }
    }

    private static void load(PlayerDataRepositoryImpl<String> repository, UUID player) throws Exception {
        var load = PlayerDataRepositoryImpl.class.getDeclaredMethod("loadDataAndLock", UUID.class, BooleanSupplier.class);
        load.setAccessible(true);
        require("fresh".equals(load.invoke(repository, player, (BooleanSupplier) () -> true)), "Cannot load player");
    }

    private static void require(boolean condition, String reason) {
        if (!condition) throw new AssertionError(reason);
    }

    private static void await(BooleanSupplier condition) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
        while (!condition.getAsBoolean()) {
            require(System.nanoTime() < until, "Operation timed out");
            Thread.sleep(10);
        }
    }
}
