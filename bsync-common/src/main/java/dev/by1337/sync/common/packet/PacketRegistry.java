package dev.by1337.sync.common.packet;

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PacketRegistry {
    public static final int MAX_COUNT = 1 << 16;
    private final Object2IntOpenHashMap<Class<? extends Packet>> type2id = new Object2IntOpenHashMap<>();
    private final List<PacketFactory<? extends Packet>> id2factory = new ArrayList<>();
    private final List<Class<? extends Packet>> packets = new ArrayList<>();


    public <T extends Packet> PacketRegistry add(Class<T> type, PacketFactory<T> f) {
        if (id2factory.size() >= MAX_COUNT)
            throw new IllegalStateException();
        int id = id2factory.size();
        id2factory.add(f);
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
    public List<Class<? extends Packet>> getPackets(){
        return Collections.unmodifiableList(packets);
    }
}
