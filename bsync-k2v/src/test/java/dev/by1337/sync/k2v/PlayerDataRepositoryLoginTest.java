package dev.by1337.sync.k2v;

import dev.by1337.sync.k2v.storage.PlayerDataStorage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class PlayerDataRepositoryLoginTest {
    private final UUID key = UUID.randomUUID();
    private PlayerDataStorage storage;
    private DataManager<String> manager;
    private PlayerDataRepositoryImpl<String> repository;
    private Method load;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws Exception {
        storage = mock(PlayerDataStorage.class);
        when(storage.isLocked(any())).thenReturn(true);
        manager = mock(DataManager.class);
        repository = new PlayerDataRepositoryImpl<>(storage, null, manager);
        load = PlayerDataRepositoryImpl.class.getDeclaredMethod("loadDataAndLock", UUID.class, BooleanSupplier.class);
        load.setAccessible(true);
        when(manager.read(any(), eq(key))).thenReturn("loaded");
    }

    private String load(BooleanSupplier connected) throws Exception {
        return (String) load.invoke(repository, key, connected);
    }

    @Test
    void failedLockRemovesReservationAndAllowsNextLogin() throws Exception {
        when(storage.lockAndLoadData(eq(key), any())).thenAnswer(call -> {
            call.<BiConsumer<Boolean, byte[]>>getArgument(1).accept(false, null);
            return 7;
        });
        assertNull(load(() -> true));
        assertNull(repository.getUser(key));
        verify(storage).unlock(key, 7);
        when(storage.lockAndLoadData(eq(key), any())).thenAnswer(call -> {
            call.<BiConsumer<Boolean, byte[]>>getArgument(1).accept(true, new byte[]{ 1 });
            return 8;
        });
        assertEquals("loaded", load(() -> true));
        assertEquals("loaded", repository.getUser(key));
    }

    @Test
    void deserializationFailureImmediatelyReleasesLock() throws Exception {
        when(manager.read(any(), eq(key))).thenThrow(new IllegalArgumentException("bad data"));
        when(storage.lockAndLoadData(eq(key), any())).thenAnswer(call -> {
            call.<BiConsumer<Boolean, byte[]>>getArgument(1).accept(true, new byte[]{ 1 });
            return 9;
        });
        assertNull(load(() -> true));
        verify(storage).unlock(key, 9);
        assertNull(repository.getUser(key));
    }

    @Test
    void disconnectDuringLoadCannotPublishPlayerData() throws Exception {
        var connected = new AtomicBoolean(true);
        when(storage.lockAndLoadData(eq(key), any())).thenAnswer(call -> {
            connected.set(false);
            call.<BiConsumer<Boolean, byte[]>>getArgument(1).accept(true, new byte[]{ 1 });
            return 10;
        });
        assertNull(load(connected::get));
        verify(storage).unlock(key, 10);
        verify(manager, never()).read(any(), any());
    }

    @Test
    void interruptedLoginCannotBeResurrectedByLateStorageResponse() throws Exception {
        var callback = new AtomicReference<BiConsumer<Boolean, byte[]>>();
        var requested = new CountDownLatch(1);
        when(storage.lockAndLoadData(eq(key), any())).thenAnswer(call -> {
            callback.set(call.getArgument(1));
            requested.countDown();
            return 11;
        });
        var result = new CompletableFuture<String>();
        var worker = new Thread(() -> {
            try {
                result.complete(load(() -> true));
            } catch (Exception error) {
                result.completeExceptionally(error);
            }
        });
        worker.start();
        try {
            assertTrue(requested.await(5, TimeUnit.SECONDS));
            worker.interrupt();
            assertNull(result.get(5, TimeUnit.SECONDS));
            callback.get().accept(true, new byte[]{ 1 });
            assertNull(repository.getUser(key));
            verify(storage).unlock(key, 11);
            verify(manager, never()).read(any(), any());
        } finally {
            worker.interrupt();
            worker.join(5_000);
        }
    }

    @Test
    void closingRepositoryRejectsLateLoadResponse() throws Exception {
        when(storage.lockAndLoadData(eq(key), any())).thenAnswer(call -> {
            repository.close();
            call.<BiConsumer<Boolean, byte[]>>getArgument(1).accept(true, new byte[]{ 1 });
            return 12;
        });
        assertNull(load(() -> true));
        assertNull(repository.getUser(key));
        verify(manager, never()).read(any(), any());
    }

    @Test
    void replacingSnapshotPreservesLoadedPlayerOwnership() throws Exception {
        when(storage.lockAndLoadData(eq(key), any())).thenAnswer(call -> {
            call.<BiConsumer<Boolean, byte[]>>getArgument(1).accept(true, new byte[]{ 1 });
            return 13;
        });
        assertEquals("loaded", load(() -> true));
        repository.pushSnapshot(key, "updated");
        assertEquals("updated", repository.getUser(key));
        // The same connection must still own the player, so a duplicate login fails.
        assertNull(load(() -> true));
        assertEquals("updated", repository.getUser(key));
        verify(storage, times(1)).lockAndLoadData(eq(key), any());
    }

    @Test
    void loadedPlayerMailStillUsesDurableStorageInsteadOfDirectCallback() throws Exception {
        when(storage.lockAndLoadData(eq(key), any())).thenAnswer(call -> {
            call.<BiConsumer<Boolean, byte[]>>getArgument(1).accept(true, new byte[]{ 1 });
            return 14;
        });
        assertEquals("loaded", load(() -> true));
        repository.pushMail(key, "reward");
        verify(storage).pushMail(key, "reward");
        verify(manager, never()).acceptMail(anyString(), anyString(), any(), any());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    void stableIdReachesDataManagerAndHandlerFailurePropagatesForRetry() throws Exception {
        when(storage.lockAndLoadData(eq(key), any())).thenAnswer(call -> {
            call.<BiConsumer<Boolean, byte[]>>getArgument(1).accept(true, new byte[]{ 1 });
            return 15;
        });
        assertEquals("loaded", load(() -> true));
        var captor = org.mockito.ArgumentCaptor.forClass(java.util.function.Consumer.class);
        verify(storage).setMailHandler(captor.capture());
        var callback = (java.util.function.Consumer<dev.by1337.sync.common.mail.Mail>) captor.getValue();
        var id = UUID.randomUUID();
        var mail = new dev.by1337.sync.common.mail.Mail(id, key, "reward");
        callback.accept(mail);
        verify(manager).acceptMail("loaded", "reward", key, id);
        doThrow(new IllegalStateException("temporary failure"))
                .when(manager).acceptMail("loaded", "reward", key, id);
        assertThrows(IllegalStateException.class, () -> callback.accept(mail));
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    void lockLossInvalidatesOnlyTheMatchingSessionAndPreventsStaleWrites() throws Exception {
        when(storage.lockAndLoadData(eq(key), any())).thenAnswer(call -> {
            call.<BiConsumer<Boolean, byte[]>>getArgument(1).accept(true, new byte[]{ 1 });
            return 16;
        });
        assertEquals("loaded", load(() -> true));
        var captor = org.mockito.ArgumentCaptor.forClass(BiConsumer.class);
        verify(storage).setLockLostHandler(captor.capture());
        BiConsumer<UUID, Integer> callback = captor.getValue();
        callback.accept(key, 15);
        assertEquals("loaded", repository.getUser(key));
        verify(manager, never()).forceUnlock(any());
        callback.accept(key, 16);
        assertNull(repository.getUser(key));
        verify(manager).forceUnlock(key);
        repository.pushSnapshot(key, "stale");
        verify(storage, never()).pushSnapshot(any(), any());
        assertNull(load(() -> true));
        verify(storage, times(1)).lockAndLoadData(eq(key), any());
    }
}
