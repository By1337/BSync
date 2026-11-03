package dev.by1337.sync.client.channel;

import dev.by1337.sync.client.config.ConnectionConfig;
import dev.by1337.sync.client.network.Connection;
import dev.by1337.sync.common.transfer.*;
import dev.by1337.sync.common.work.EventLoopWorkers;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class ServerGroupExportTest {
    @Test
    void exportIndicesMatchTheActualSortedRouterArray() {
        var workers = new EventLoopWorkers("group-export-test-%d", 1);
        var connections = List.of(
                new Connection(new ConnectionConfig("group", "third", 1000, "key-three"), workers, "client", null),
                new Connection(new ConnectionConfig("group", "first", 1000, "key-one"), workers, "client", null),
                new Connection(new ConnectionConfig("group", "second", 1000, "key-two"), workers, "client", null));
        class Storage implements TransferStorage {
            final String name;
            ShardSelection selection;

            Storage(String name) {
                this.name = name;
            }

            public List<ExportSource> exportSources(ShardSelection shard) {
                selection = shard;
                return List.of();
            }
        }
        var group = new ServerGroup<Storage>(connections, connection -> new ChannelMaker.ChannelData<>() {
            final Storage storage = new Storage(connection.config().ip());

            public Storage get() {
                return storage;
            }

            public void close() {
            }
        });
        var ordered = group.channels().stream().map(ChannelMaker.ChannelData::get).toList();
        TransferStorage.shardedExportSources(ordered);
        var expectedNames = connections.stream().sorted(Comparator.comparing(c -> c.config().serverId())).map(c -> c.config().ip()).toList();
        for (int index = 0; index < ordered.size(); index++) {
            assertEquals(expectedNames.get(index), ordered.get(index).name);
            assertEquals(new ShardSelection(3, index), ordered.get(index).selection);
        }
        var random = new Random(2026);
        var keys = new ArrayList<UUID>();
        keys.add(new UUID(Long.MIN_VALUE, 0));
        for (int i = 0; i < 500; i++) keys.add(new UUID(random.nextLong(), random.nextLong()));
        for (UUID key : keys) {
            int expected = Math.floorMod(key.getMostSignificantBits() ^ key.getLeastSignificantBits(), ordered.size());
            assertSame(ordered.get(expected), group.route(key));
            assertTrue(group.route(key).selection.includes(key));
        }
    }
}
