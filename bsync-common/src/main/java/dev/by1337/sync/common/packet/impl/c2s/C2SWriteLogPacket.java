package dev.by1337.sync.common.packet.impl.c2s;

import dev.by1337.sync.common.packet.ByteBufCodecs;
import dev.by1337.sync.common.packet.Packet;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class C2SWriteLogPacket implements Packet {
    public static final int MAX_LOG_SIZE = 4 << 10;
    public static final int MAX_KEYWORD_LENGTH = 64;
    public static final int MAX_KEYWORD_COUNT = 64;
    private final long timestamp;
    private final byte[] log;
    private final String[] keywords;

    public C2SWriteLogPacket(String log, String... keywords) {
        this(System.currentTimeMillis(), log, keywords);
    }
    public C2SWriteLogPacket(long timestamp, String log, String... keywords) {
        this(timestamp, log.getBytes(StandardCharsets.UTF_8), keywords);
    }
    public C2SWriteLogPacket(long timestamp, byte[] log, String... keywords) {
        this.timestamp = timestamp;
        if (log.length > MAX_LOG_SIZE){
            log = Arrays.copyOf(log, MAX_LOG_SIZE);
        }
        this.log = log;
        this.keywords = new String[Math.min(keywords.length, MAX_KEYWORD_COUNT)];
        for (int i = 0; i < this.keywords.length; i++) {
            var s = keywords[i];
            this.keywords[i] = s.toLowerCase().substring(0, Math.min(s.length(), MAX_KEYWORD_LENGTH));
        }
    }

    public static C2SWriteLogPacket read(ByteBuf buf, int protocol){
        long timestamp = buf.readLong();
        byte[] log = ByteBufCodecs.readByteArray(buf);
        String[] keywords = new String[buf.readByte()];
        for (int i = 0; i < keywords.length; i++) {
            keywords[i] = ByteBufCodecs.readUtf8(buf);
        }
        return new C2SWriteLogPacket(timestamp, log, keywords);
    }

    @Override
    public void write(ByteBuf buf, int protocolVersion) {
        buf.writeLong(timestamp);
        ByteBufCodecs.writeByteArray(buf, log);
        assert keywords.length < 128 : "keywords count > 128";
        buf.writeByte(keywords.length);
        for (String keyword : keywords) {
            ByteBufCodecs.writeUtf8(buf, keyword);
        }
    }

    public long timestamp() {
        return timestamp;
    }

    public byte[] log() {
        return log;
    }

    public String[] keywords() {
        return keywords;
    }
}
