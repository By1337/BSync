package dev.by1337.sync.common.channel;

public enum ChannelType {
    LOCKS(0),
    CUSTOM(1),
    ;
    public final byte id;

    ChannelType(int id) {
        this.id = (byte) id;
    }
    public static ChannelType fromId(int id) {
        if (id == 0)
            return LOCKS;
        if (id == 1)
            return CUSTOM;
        throw new IllegalArgumentException("Unknown channel type: " + id);
    }
}
