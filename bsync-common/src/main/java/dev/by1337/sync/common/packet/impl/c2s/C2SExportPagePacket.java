package dev.by1337.sync.common.packet.impl.c2s;

import dev.by1337.sync.common.packet.*;
import dev.by1337.sync.common.packet.impl.s2c.S2CExportPagePacket;
import dev.by1337.sync.common.transfer.TransferEntry.Kind;
import dev.by1337.sync.common.transfer.ShardSelection;
import io.netty.buffer.ByteBuf;
import java.util.UUID;

public record C2SExportPagePacket(Kind kind, UUID afterKey, long afterSequence, int shardCount, int shardIndex) implements Packet, ExpectsResponse<S2CExportPagePacket> {
    public C2SExportPagePacket {
        new ShardSelection(shardCount, shardIndex);
    }

    public C2SExportPagePacket(Kind kind, UUID afterKey, long afterSequence) {
        this(kind, afterKey, afterSequence, 1, 0);
    }

    public ShardSelection shard() {
        return new ShardSelection(shardCount, shardIndex);
    }

    public C2SExportPagePacket(ByteBuf buf, int version) {
        this(Kind.values()[buf.readUnsignedByte()], ByteBufCodecs.readOptional(buf, ByteBufCodecs::readUUID), buf.readLong(), buf.readInt(), buf.readInt());
    }

    @Override
    public void write(ByteBuf buf, int version) {
        buf.writeByte(kind.ordinal());
        ByteBufCodecs.writeOptional(buf, afterKey, ByteBufCodecs::writeUUID);
        buf.writeLong(afterSequence);
        buf.writeInt(shardCount);
        buf.writeInt(shardIndex);
    }
}
