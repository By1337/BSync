package dev.by1337.sync.k2v.storage;

import dev.by1337.sync.client.channel.handler.lock.Locks;
import dev.by1337.sync.common.packet.impl.s2c.S2CExportPagePacket;
import dev.by1337.sync.k2v.transfer.*;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class BSyncStorageTransferTest {
    @Test
    void exportConvertsNetworkDtosAndPreservesShardCursorAndMailId() throws Exception {
        Locks locks = mock(Locks.class);
        var network = mock(dev.by1337.sync.common.transfer.ExportSource.class);
        UUID player = UUID.randomUUID(), mail = UUID.randomUUID();
        var entry = new dev.by1337.sync.common.transfer.TransferEntry(
                dev.by1337.sync.common.transfer.TransferEntry.Kind.MAIL, player, new byte[]{ 1 }, mail);
        when(locks.exportSources()).thenReturn(List.of(network));
        when(network.next(entry.kind())).thenReturn(CompletableFuture.completedFuture(
                new S2CExportPagePacket(List.of(entry), player, 42, false, "")));
        var storage = new BSyncStorage();
        storage.setLocks(locks, () -> {
        });
        var source = storage.exportSources().get(0);
        var page = source.next(TransferEntry.Kind.MAIL).get();
        assertEquals(player, page.afterKey());
        assertEquals(42, page.afterSequence());
        assertFalse(page.done());
        var converted = page.entries().get(0);
        assertEquals(TransferEntry.Kind.MAIL, converted.kind());
        assertEquals(player, converted.key());
        assertEquals(mail, converted.messageId());
        assertArrayEquals(new byte[]{ 1 }, converted.payload());
        source.close();
        verify(network).close();
    }

    @Test
    void importPreservesMessageIdentityAndNetworkResult() throws Exception {
        Locks locks = mock(Locks.class);
        var storage = new BSyncStorage();
        storage.setLocks(locks, () -> {
        });
        when(locks.importEntry(any())).thenReturn(CompletableFuture.completedFuture(
                new dev.by1337.sync.common.transfer.TransferResult(dev.by1337.sync.common.transfer.TransferResult.Status.LOCKED, "busy")));
        var entry = new TransferEntry(TransferEntry.Kind.MAIL, UUID.randomUUID(), new byte[]{ 2 }, UUID.randomUUID());
        var result = storage.importEntry(entry).get();
        assertEquals(TransferResult.Status.LOCKED, result.status());
        assertEquals("busy", result.detail());
        var captor = org.mockito.ArgumentCaptor.forClass(dev.by1337.sync.common.transfer.TransferEntry.class);
        verify(locks).importEntry(captor.capture());
        assertEquals(entry.key(), captor.getValue().key());
        assertEquals(entry.messageId(), captor.getValue().messageId());
        assertArrayEquals(entry.payload(), captor.getValue().payload());
    }
}
