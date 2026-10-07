package dev.by1337.sync.common.packet.impl.c2s;

import dev.by1337.sync.common.packet.ExpectsResponse;
import dev.by1337.sync.common.packet.Packet;
import dev.by1337.sync.common.packet.impl.a2a.A2ALongResponse;
import io.netty.buffer.ByteBuf;

public record C2SGiveMailUidsRequest() implements Packet, ExpectsResponse<A2ALongResponse> {
    public static final int RANGE_SIZE = 1_000_000;

    public C2SGiveMailUidsRequest(ByteBuf buf, int protocolVersion) {
        this();
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
    }
}
