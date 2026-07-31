package dev.by1337.sync.common.packet;

import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.jetbrains.annotations.NotNull;

import java.util.*;

public class PacketRegistries implements Iterable<PacketRegistry> {
    private final Map<String, PacketRegistry> name2registry = new HashMap<>();
    private final Int2ObjectOpenHashMap<PacketRegistry> id2registry = new Int2ObjectOpenHashMap<>();
    private final Object2ObjectOpenHashMap<Class<? extends Packet>, PacketRegistry> packet2registry = new Object2ObjectOpenHashMap<>();
    private final Object2IntOpenHashMap<PacketRegistry> registry2id = new Object2IntOpenHashMap<>();
    private final Int2IntOpenHashMap registryId2version = new Int2IntOpenHashMap();

    public PacketRegistries add(int id, String name, PacketRegistry registry) {
        return add(id, name, registry, registry.latestVersion());
    }

    public PacketRegistries add(int id, String name, PacketRegistry registry, int version) {
        if (name2registry.containsKey(name)) throw new IllegalStateException("Registry " + name + " is already exists");
        if (id2registry.containsKey(id))
            throw new IllegalStateException("Registry " + id + " is already exists " + name);
        id2registry.put(id, registry);
        name2registry.put(name, registry);
        registryId2version.put(id, version);
        for (Class<? extends Packet> packet : registry.getPackets()) {
            packet2registry.put(packet, registry);
        }
        registry2id.put(registry, id);
        return this;
    }

    public int getVersion(PacketRegistry registry) {
        return getVersion(getRegistryId(registry));
    }

    public int getVersion(int id) {
        return registryId2version.get(id);
    }

    public PacketRegistry getRegistry(String name) {
        return Objects.requireNonNull(name2registry.get(name), "Unknown registry " + name);
    }

    public PacketRegistry getRegistry(int id) {
        if (id < 0 || id2registry.size() <= id) throw new IllegalArgumentException("Unknown registry " + id);
        return Objects.requireNonNull(id2registry.get(id), "Unknown registry " + id);
    }

    public PacketRegistry getRegistry(Class<? extends Packet> packet) {
        return Objects.requireNonNull(packet2registry.get(packet), "Unregistered packet " + packet);
    }

    public int getRegistryId(PacketRegistry registry) {
        int id = registry2id.getInt(registry);
        if (id == -1) throw new IllegalArgumentException("Unregistered registry " + id);
        return id;
    }

    @Override
    public @NotNull Iterator<PacketRegistry> iterator() {
        return name2registry.values().iterator();
    }

    public Snapshot createSnapshot() {
        List<Snapshot.RegistryData> registryData = new ArrayList<>();
        for (PacketRegistry registry : this) {
            registryData.add(new Snapshot.RegistryData(
                    getRegistryId(registry),
                    registry.id(),
                    getVersion(registry)
            ));
        }
        return new Snapshot(registryData);
    }

    public boolean isLikeA(Snapshot snapshot) {
        for (Snapshot.RegistryData r : snapshot.registryData) {
            var v = name2registry.get(r.name);
            if (v == null) return false;
            int id = getRegistryId(v);
            var version = getVersion(id);
            if (id != r.id) return false;
            if (version < r.version) return false;
        }
        return true;
    }

    public record Snapshot(List<RegistryData> registryData) {
        public void write(ByteBuf buf) {
            buf.writeInt(registryData.size());
            for (RegistryData data : registryData) {
                buf.writeInt(data.id);
                ByteBufCodecs.writeUtf8(buf, data.name);
                buf.writeInt(data.version);
            }
        }

        public static Snapshot read(ByteBuf buf) {
            List<RegistryData> registryData = new ArrayList<>();
            int count = buf.readInt();
            for (int i = 0; i < count; i++) {
                int id = buf.readInt();
                String name = ByteBufCodecs.readUtf8(buf);
                int version = buf.readInt();
                registryData.add(new RegistryData(id, name, version));
            }
            return new Snapshot(registryData);
        }

        public record RegistryData(int id, String name, int version) {}
    }
}
