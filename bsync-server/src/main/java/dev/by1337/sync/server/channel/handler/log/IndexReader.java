package dev.by1337.sync.server.channel.handler.log;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class IndexReader {
    private final byte[][] keywords;
    private final long after;
    private final ByteBuffer logBuf = ByteBuffer.allocateDirect(512);

    public IndexReader(byte[][] keywords, long after) {
        this.keywords = keywords;
        this.after = after;
    }

    public static void main(String[] args) throws Exception {
        IndexReader reader = new IndexReader(new byte[][]{
                "by1337".getBytes(StandardCharsets.UTF_8),
                "night".getBytes(StandardCharsets.UTF_8),
        }, 0);
        Path base = Path.of("/home/by1337/tmp/test/");
        Path logFile = base.resolve("2026-06-06.log");
        Path path = base.resolve(".indexes/2026-06-06.idx");

        try (
                FileChannel ch = FileChannel.open(path, StandardOpenOption.READ);
                FileChannel logs = FileChannel.open(logFile, StandardOpenOption.READ);
        ) {
            reader.read(ch, logs);
        }
    }

    public void read(FileChannel ch, FileChannel logs) throws Exception {
        long statNanos = System.nanoTime();
        final short[] targets = new short[keywords.length];
        int requiredMask = 0;
        for (int i = 0; i < targets.length; i++) {
            requiredMask |= (1 << i);
        }
        Arrays.fill(targets, (short) -1);
        logBuf.clear();

        int dataBlocks = 0;
        int magicBlocks = 0;
        int dictBlocks = 0;
        int errors = 0;

        boolean isValid = true;

        long offsets = 0;
        long timestamp = 0;
        byte[] keyword = new byte[256];

        MappedByteBuffer buf =
                ch.map(
                        FileChannel.MapMode.READ_ONLY,
                        0,
                        ch.size()
                );
        reader:
        while (true){
            if (!isValid) errors++;
            while (!isValid) {
                if (buf.remaining() < 1 + 4 + 8 + 8) break reader;
                if (buf.get() == LogStorage.MAGIC_BLOCK) {
                    if (buf.getInt() == LogStorage.MAGIC) {
                        offsets = buf.getLong();
                        timestamp = buf.getLong();
                        if (offsets > 0 && timestamp > 0) {
                            isValid = true;
                            continue reader;
                        }
                    }
                }
            }
            if (buf.remaining() < 2) break reader;
            byte type = buf.get();
            switch (type){
                case LogStorage.MAGIC_BLOCK -> {
                    magicBlocks++;
                    if (buf.remaining() < 4 + 8 + 8) break reader;
                    if (buf.getInt() == LogStorage.MAGIC) {
                        offsets = buf.getLong();
                        timestamp = buf.getLong();
                        if (offsets < 0 || timestamp < 0) {
                            isValid = false;
                        }
                    }else {
                        isValid = false;
                    }
                }
                case LogStorage.DICT_BLOCK -> {
                    dictBlocks++;
                    int payload_size = buf.get();
                    if ((payload_size & 0x80) == 128) {
                        if (buf.hasRemaining()) {
                            payload_size = (payload_size & 0x7F) | ((buf.get() & 0x7F) << 7);
                        } else {
                            break reader;
                        }
                    }
                    if (payload_size < 0 || payload_size > 256 + 3) {
                        isValid = false;
                        break;
                    }
                    if (buf.remaining() < payload_size) {
                        break reader;
                    }
                    short id = buf.getShort();
                    int length = Byte.toUnsignedInt(buf.get());
                    if (id < 0 || buf.remaining() < length) {
                        isValid = false;
                        break;
                    }
                    buf.get(keyword, 0, length);
                    for (int i = 0; i < keywords.length; i++) {
                        var a = keywords[i];
                        if (a.length == length && equals(a, keyword)) {
                            targets[i] = id;
                        } else if (targets[i] == id) {
                            targets[i] = -1;
                        }
                    }
                }
                case LogStorage.DATA_BLOCK -> {
                    dataBlocks++;
                    int payload_size = Byte.toUnsignedInt(buf.get());
                    if (buf.remaining() < payload_size) break reader;
                    int offset_delta = ByteBufUtil.readVarInt(buf);
                    if (offset_delta < 0) {
                        isValid = false;
                        break;
                    }
                    offsets = offsets + offset_delta;
                    int length = Byte.toUnsignedInt(buf.get());
                    if (buf.remaining() < length * 2) {
                        isValid = false;
                        break;
                    }
                    if (timestamp < after) {
                        buf.position(buf.position() + 2 * length);
                        continue;
                    }
                    int mask = 0;
                    for (int i = 0; i < length; i++) {
                        short id = buf.getShort();
                        for (int i1 = 0; i1 < targets.length; i1++) {
                            if (targets[i1] == id) {
                                mask |= (1 << i1);
                            }
                        }
                    }
                    if (mask == requiredMask) {
                        logBuf.clear();
                        var line = readLogLine(logs, logBuf, offsets);
                        if (line == null) {
                            isValid = false;
                            break;
                        }
                        if (magicBlocks > 1)
                            System.out.println(line);
                    }
                }
            }

        }
        System.out.println(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - statNanos) + "ms");
        System.out.println("dataBlocks:  " + dataBlocks);
        System.out.println("magicBlocks: " + magicBlocks);
        System.out.println("dictBlocks:  " + dictBlocks);
        System.out.println("errors:      " + errors);
    }

    private static boolean equals(byte[] a, byte[] a1) {
        if (a1.length < a.length) return false;
        for (int i = 0; i < a.length; i++) {
            if (a[i] != a1[i]) return false;
        }
        return true;
    }

    private static String readLogLine(FileChannel logs, ByteBuffer buf, long position) throws IOException {
        if (position + 2 >= logs.size()) return null;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        int read;
        int totalRead = 0;
        while ((read = logs.read(buf, position)) != -1) {
            totalRead += read;
            position += read;
            buf.flip();
            while (buf.remaining() >= 1) {
                byte b = buf.get();
                if (b == 0x0A) {
                    if (bos.size() == 0) return null;
                    return bos.toString(StandardCharsets.UTF_8);
                } else {
                    bos.write(b);
                }
            }
            buf.compact();
            if (totalRead >= 10 << 10) {
                return null;
            }
        }
        return null;
    }
}
