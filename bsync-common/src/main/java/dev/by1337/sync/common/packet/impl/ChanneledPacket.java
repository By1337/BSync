package dev.by1337.sync.common.packet.impl;

import dev.by1337.sync.common.packet.ByteBufCodecs;
import dev.by1337.sync.common.packet.ChannelRegistryContext;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;

public record ChanneledPacket(String id, Packet payload) implements Packet {

    public static ChanneledPacket read(ByteBuf buf, int protocolVersion) {
        var id = ByteBufCodecs.readUtf8(buf);

        try (var ignored = setupRegistries(id)) {
            var payload = Packets.readGlobal(buf, protocolVersion);
            return new ChanneledPacket(id, payload);
        }
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        try (var ignored = setupRegistries(id)) {
            ByteBufCodecs.writeUtf8(buf, id);
            Packets.writeGlobal(buf, protocolVersion, payload);
        }
    }

    private static ChannelRegistryContext.Scope setupRegistries(String id) {
        var channel = ChannelRegistryContext.getCurrentNettyChannel();
        if (channel == null) throw new IllegalStateException("Channel is null");
        var registries = channel.attr(ChannelRegistryContext.CHANNEL_REGISTRY_CONTEXT).get();
        if (registries == null) throw new IllegalStateException("Channel has no registries");
        return registries.setCurrentChannel(id);
    }
}
