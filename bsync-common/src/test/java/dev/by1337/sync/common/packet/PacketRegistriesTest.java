package dev.by1337.sync.common.packet;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.testng.annotations.Test;
import java.util.List;
import static org.testng.Assert.*;

public class PacketRegistriesTest {
    private PacketRegistries registries() {
        return new PacketRegistries().add(0, "main", new PacketRegistry("main", 3));
    }

    @Test
    public void rejectsDuplicateNamesAndIds() {
        PacketRegistries registries = registries();
        assertThrows(IllegalStateException.class,
                () -> registries.add(1, "main", new PacketRegistry("main", 3)));
        assertThrows(IllegalStateException.class,
                () -> registries.add(0, "other", new PacketRegistry("other", 1)));
        assertEquals(registries.createSnapshot().registryData().size(), 1);
        assertSame(registries.getRegistry(0), registries.getRegistry("main"));
    }

    @Test
    public void compatibilityRequiresKnownNameMatchingIdAndSupportedVersion() {
        PacketRegistries registries = registries();
        assertTrue(registries.isLikeA(snapshot(0, "main", 3)));
        assertTrue(registries.isLikeA(snapshot(0, "main", 2)));
        assertFalse(registries.isLikeA(snapshot(0, "main", 4)));
        assertFalse(registries.isLikeA(snapshot(1, "main", 3)));
        assertFalse(registries.isLikeA(snapshot(0, "unknown", 3)));
    }

    @Test
    public void snapshotPreservesNegotiatedVersionAndSurvivesSerialization() {
        PacketRegistries registries = new PacketRegistries()
                .add(0, "main", new PacketRegistry("main", 3), 2)
                .add(1, "locks", new PacketRegistry("locks", 1));
        assertEquals(registries.getVersion(0), 2);
        PacketRegistries.Snapshot snapshot = registries.createSnapshot();
        assertTrue(snapshot.registryData().contains(new PacketRegistries.Snapshot.RegistryData(0, "main", 2)));
        assertTrue(snapshot.registryData().contains(new PacketRegistries.Snapshot.RegistryData(1, "locks", 1)));
        ByteBuf buf = Unpooled.buffer();
        try {
            snapshot.write(buf);
            assertEquals(PacketRegistries.Snapshot.read(buf), snapshot);
            assertEquals(buf.readableBytes(), 0);
        } finally {
            buf.release();
        }
    }

    private PacketRegistries.Snapshot snapshot(int id, String name, int version) {
        return new PacketRegistries.Snapshot(List.of(
                new PacketRegistries.Snapshot.RegistryData(id, name, version)));
    }
}
