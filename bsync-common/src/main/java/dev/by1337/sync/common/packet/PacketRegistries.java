package dev.by1337.sync.common.packet;

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

import java.util.*;

public class PacketRegistries {
    private final Map<String, PacketRegistry> name2registry = new HashMap<>();
    private final List<PacketRegistry> id2registry = new ArrayList<>();
    private final Object2ObjectOpenHashMap<Class<? extends Packet>, PacketRegistry> packet2registry = new Object2ObjectOpenHashMap<>();
    private final Object2IntOpenHashMap<PacketRegistry> registry2id = new Object2IntOpenHashMap<>();


    public PacketRegistries add(String name, PacketRegistry registry){
        if (name2registry.containsKey(name)) throw new IllegalStateException("Registry " + name + " is already exists");
        int id = id2registry.size();
        id2registry.add(registry);
        name2registry.put(name, registry);
        for (Class<? extends Packet> packet : registry.getPackets()) {
            packet2registry.put(packet, registry);
        }
        registry2id.put(registry, id);
        return this;
    }
    public PacketRegistry getRegistry(String name){
        return Objects.requireNonNull(name2registry.get(name), "Unknown registry " + name);
    }
    public PacketRegistry getRegistry(int id){
        if (id < 0 || id2registry.size() <= id) throw new IllegalArgumentException("Unknown registry " + id);
        return Objects.requireNonNull(id2registry.get(id), "Unknown registry " + id);
    }
    public PacketRegistry getRegistry(Class<? extends Packet> packet    ){
        return Objects.requireNonNull(packet2registry.get(packet), "Unregistered packet " + packet);
    }
    public int getRegistryId(PacketRegistry registry){
        int id = registry2id.getInt(registry);
        if (id == -1) throw new IllegalArgumentException("Unregistered registry " + id);
        return id;
    }
}
