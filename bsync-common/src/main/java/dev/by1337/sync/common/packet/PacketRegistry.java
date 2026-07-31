package dev.by1337.sync.common.packet;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PacketRegistry {
    public static final int MAX_COUNT = 1 << 16;
    private final Object2IntOpenHashMap<Class<? extends Packet>> type2id = new Object2IntOpenHashMap<>();
    private final Int2ObjectOpenHashMap<PacketFactory<? extends Packet>> id2factory = new Int2ObjectOpenHashMap<>();
    private final List<Class<? extends Packet>> packets = new ArrayList<>();
    private final String id;
    private final int latestVersion;
    private boolean lock;

    public PacketRegistry(String id, int latestVersion) {
        this.id = id;
        this.latestVersion = latestVersion;
    }


    public <T extends Packet> PacketRegistry add(int id, Class<T> type, PacketFactory<T> f) {
        if (lock) throw new IllegalStateException("Registry is locked!");
        if (id >= MAX_COUNT)
            throw new IllegalStateException();
        if (id2factory.containsKey(id)) throw new IllegalStateException("Duplicate id: " + id + " " + type);
        id2factory.put(id, f);
        packets.add(type);
        type2id.put(type, id);
        return this;
    }

    public int getId(Class<? extends Packet> c) {
        var v = type2id.getInt(c);
        if (v == -1) throw new IllegalArgumentException("unregistered packet " + c);
        return v;
    }

    public PacketFactory<? extends Packet> getFactory(int id) {
        var v = id2factory.get(id);
        if (v == null) throw new IllegalStateException("Bad packet id " + id);
        return v;
    }

    public List<Class<? extends Packet>> getPackets() {
        return Collections.unmodifiableList(packets);
    }

    public PacketRegistry lock() {
        lock = true;
        return this;
    }

    public String id() {
        return id;
    }

    public int latestVersion() {
        return latestVersion;
    }
}
