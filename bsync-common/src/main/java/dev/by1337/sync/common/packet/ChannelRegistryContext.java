package dev.by1337.sync.common.packet;

import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ChannelRegistryContext {
    private static final Map<String, PacketRegistries> channels = new ConcurrentHashMap<>();

    private static final ThreadLocal<PacketRegistries> CURRENT_REGISTRY = new ThreadLocal<>();

    public static @Nullable PacketRegistries getByChannel(String channel) {
        return channels.get(channel);
    }

    public static void onChannelOpen(String id, PacketRegistries registries) {
        if (channels.putIfAbsent(id, registries) != null) {
            throw new IllegalStateException("Duplicate channel id " + id);
        }
    }

    public static void onChannelClose(String id) {
        channels.remove(id);
    }

    public static Scope setCurrentChannel(String id){
        var v = channels.get(id);
        if (v == null){
            throw new IllegalStateException("Unknown channel id " + id);
        }
        var old = CURRENT_REGISTRY.get();
        CURRENT_REGISTRY.set(v);
        return () -> {
            if (old == null)
                CURRENT_REGISTRY.remove();
            else
                CURRENT_REGISTRY.set(old);
        };
    }
    public static @Nullable PacketRegistries getCurrentChannel() {
        return CURRENT_REGISTRY.get();
    }

    @FunctionalInterface
    public interface Scope extends AutoCloseable {
        @Override
        void close();
    }
}
