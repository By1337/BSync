package dev.by1337.sync.common.packet;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@ApiStatus.Internal
public final class ChannelRegistryContext {
    public static final AttributeKey<ChannelRegistryContext>  CHANNEL_REGISTRY_CONTEXT = AttributeKey.newInstance("channelRegistryContext");

    private static final ThreadLocal<PacketRegistries> CURRENT_REGISTRY = new ThreadLocal<>();
    private static final ThreadLocal<Channel> CURRENT_NETTY_CHANNEL = new ThreadLocal<>();

    private final Map<String, PacketRegistries> channels = new ConcurrentHashMap<>();

    public @Nullable PacketRegistries getByChannel(String channel) {
        return channels.get(channel);
    }

    public void onChannelOpen(String id, PacketRegistries registries) {
        if (channels.putIfAbsent(id, registries) != null) {
            throw new IllegalStateException("Duplicate channel id " + id);
        }
    }

    public void onChannelClose(String id) {
        channels.remove(id);
    }

    public Scope setCurrentChannel(String id){
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
    public static Scope setCurrentNettyChannel(Channel channel){
        var old = CURRENT_NETTY_CHANNEL.get();
        CURRENT_NETTY_CHANNEL.set(channel);
        return () -> {
            if (old == null)
                CURRENT_NETTY_CHANNEL.remove();
            else
                CURRENT_NETTY_CHANNEL.set(old);
        };
    }
    public static @Nullable Channel getCurrentNettyChannel(){
        return CURRENT_NETTY_CHANNEL.get();
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
