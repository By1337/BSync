package dev.by1337.sync.common.packet.impl.s2c;

import dev.by1337.sync.common.packet.*;
import dev.by1337.sync.common.transfer.TransferResult;
import io.netty.buffer.ByteBuf;

public record S2CImportResultPacket(TransferResult.Status status, String detail) implements Packet {
    public S2CImportResultPacket(ByteBuf buf, int version) {
        this(TransferResult.Status.values()[buf.readUnsignedByte()], ByteBufCodecs.readUtf8(buf));
    }

    @Override
    public void write(ByteBuf buf, int version) {
        buf.writeByte(status.ordinal());
        ByteBufCodecs.writeUtf8(buf, detail);
    }
}
