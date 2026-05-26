package dev.by1337.sync.common.packet.impl;

import dev.by1337.sync.common.packet.ExpectsResponse;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.Packets;
import io.netty.buffer.ByteBuf;

public record AckRequest(Packet payload) implements Packet, ExpectsResponse<AckRequest.AckResponse> {

    public AckRequest(ByteBuf buf, int protocolVersion) {
        this(Packets.read(buf, protocolVersion));
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        Packets.write(buf, protocolVersion, payload);
    }

    @Override
    public int getId() {
        return Packets.ACK_REQUEST;
    }

    public record AckResponse() implements Packet{
        public static final AckResponse INSTANCE = new AckResponse();

        @Override
        public void write(ByteBuf buf, int protocolVersion) {

        }

        @Override
        public int getId() {
            return  Packets.ACK_RESPONSE;
        }
    }
}
