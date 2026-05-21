package dev.by1337.sync.common.channel;

public enum ChannelType {
    DATA_CHANNEL(0)
    ;
    public final byte id;

    ChannelType(int id) {
        this.id = (byte) id;
    }
    public static ChannelType fromId(int id) {
        if (id == 0)
            return DATA_CHANNEL;
        throw new IllegalArgumentException("Unknown channel type: " + id);
    }
}
