package dev.by1337.sync.server.channel.handler.log;

import it.unimi.dsi.fastutil.objects.Object2ShortLinkedOpenHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static java.nio.file.StandardOpenOption.*;

public final class LogStorage implements Closeable {
    private static final long MINUTE_MILLIS = TimeUnit.MINUTES.toMillis(1);
    public static final byte DATA_BLOCK = 0;
    public static final byte DICT_BLOCK = 1;
    public static final byte MAGIC_BLOCK = 2;
    public static final int MAGIC = 0xDEADBEEF;
    public static final int MAX_LINE_SIZE = 4 << 10;
    public static final int MAX_KEYWORD_SIZE = 256;


    private static final Logger log = LoggerFactory.getLogger(LogStorage.class);
    private final Path logDir;
    private final Path indexDir;

    private LocalDate currentDate;
    private long currentLocalEpochDay;
    private byte[] currentDateFormatUtf8;

    private BufferedFileChannel logWriter;
    private BufferedFileChannel indexWriter;
    private short lastKeywordId;
    private final int dictMaxSize = 2048;
    private final Object2ShortLinkedOpenHashMap<String> dict = new Object2ShortLinkedOpenHashMap<>(dictMaxSize);
    private long lastMagicBlockTimestamp;

    private final long zoneOffset;

    public LogStorage(Path root) throws IOException {
        dict.defaultReturnValue((short) -1);
        ZoneId timeZone = ZoneId.of("Europe/Moscow");
        zoneOffset = timeZone.getRules()
                .getOffset(Instant.now())
                .getTotalSeconds() * 1000L;
        this.logDir = root;
        this.indexDir = root.resolve(".indexes");

        Files.createDirectories(logDir);
        Files.createDirectories(indexDir);

        rotateLog(LocalDate.now(ZoneOffset.UTC));
    }

    public void flush() throws IOException {
        logWriter.flush(true);
        indexWriter.flush(true);
    }

    public static void main(String[] args) throws IOException {
        LogStorage logStorage = new LogStorage(Path.of("/home/by1337/tmp/test/"));
        logStorage.append(System.currentTimeMillis(), "Player by1337 pay $1000 to player2", "by1337", "economy", "player2");
        logStorage.append(System.currentTimeMillis(), "Player by1337 пиздато деньи потратил", "by1337");
        logStorage.append(System.currentTimeMillis(), "Player player2 buy diamond sword", "player2", "economy", "shop");
        logStorage.close();
    }

    private long lastOffsetPoint;

    public void append(long timestamp, String description, String... keywords) throws IOException {
        append(timestamp, description.getBytes(StandardCharsets.UTF_8), keywords);
    }
    public void append(long timestamp, byte[] description, String... keywords) throws IOException {
        rotateIfNeeded();
        updateCachedDate(timestamp);

        long offset = logWriter.writeAndGetOffset(description.length + 32, b -> {
            b.put((byte) '[');
            b.put(currentDateFormatUtf8);
            b.put((byte) '|');
            writeTime(timestamp, b);
            b.put((byte) ']');
            b.put((byte) ' ');
            b.put(description);
            b.put((byte) '\n');
        });

        short[] ids = new short[keywords.length];
        for (int i = 0; i < keywords.length; i++) {
            ids[i] = getOrCreateKeywordIndex(keywords[i]);
        }

        //[block_type:1]][payload_size:1][varInt][length:1][x(id:2)]
        int offset_delta = (int) (offset - lastOffsetPoint);
        int payload_size = ByteBufUtil.varInt3Size(offset_delta) + 1 + ids.length * 2;
        indexWriter.writeAndGetOffset(2 + payload_size, b -> {
            b.put(DATA_BLOCK);
            b.put((byte)payload_size);
            ByteBufUtil.writeVarInt(b, offset_delta);
            b.put((byte) ids.length);
            for (short id : ids) {
                b.putShort(id);
            }
        });
        lastOffsetPoint = offset;

        if (System.currentTimeMillis() - lastMagicBlockTimestamp > MINUTE_MILLIS){
            writeMagicBlock();
        }
    }

    private void writeMagicBlock() throws IOException {
        lastMagicBlockTimestamp = System.currentTimeMillis();
        indexWriter.writeAndGetOffset(5 + 8, b -> b
                .put(MAGIC_BLOCK)
                .putInt(MAGIC)
                .putLong(logWriter.offset)
                .putLong(lastMagicBlockTimestamp)
        );
        lastOffsetPoint = logWriter.offset;
    }

    private short getOrCreateKeywordIndex(String keyword) throws IOException {
        if (dict.size() - 1 >= dictMaxSize) {
            short id = dict.getShort(keyword);
            if (id != -1) return id;
            id = dict.removeLastShort();
            dict.put(keyword, id);
            writeDictBlock(id, keyword);
            return id;
        } else {
            return dict.computeIfAbsent(keyword, k -> {
                short id = lastKeywordId++;
                try {
                    writeDictBlock(id, (String) k);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return id;
            });
        }
    }

    private void writeDictBlock(short id, String keyword) throws IOException {
        byte[] arr = keyword.getBytes(StandardCharsets.UTF_8);
        int payload_size = 3 + arr.length;
        indexWriter.writeAndGetOffset(3 + payload_size, b -> {
            b.put(DICT_BLOCK);
            ByteBufUtil.writeVarInt(b, payload_size);
            b.putShort(id);
            b.put((byte) arr.length);
            b.put(arr);
        });
    }

    private void rotateIfNeeded() throws IOException {
        LocalDate now = LocalDate.now(ZoneOffset.UTC);
        if (now.toEpochDay() != currentDate.toEpochDay()) {
            rotateLog(now);
        }
    }

    private void updateCachedDate(long timestamp) {
        long localEpochDay =
                Math.floorDiv(timestamp + zoneOffset, 86400000L);

        if (localEpochDay != currentLocalEpochDay) {
            currentLocalEpochDay = localEpochDay;

            currentDateFormatUtf8 =
                    LocalDate.ofEpochDay(localEpochDay)
                            .toString()
                            .getBytes(StandardCharsets.UTF_8);
        }
    }

    private void rotateLog(LocalDate newDate) throws IOException {
        if (logWriter != null) {
            logWriter.close();
        }

        currentDate = newDate;
        String currentDateFormat = currentDate.toString();
        currentDateFormatUtf8 = currentDateFormat.getBytes(StandardCharsets.UTF_8);
        Path logFile = logDir.resolve(currentDateFormat + ".log");

        ByteBuffer buffer = logWriter != null ? logWriter.buffer : ByteBuffer.allocateDirect(8 << 10);

        logWriter = new BufferedFileChannel(
                FileChannel.open(
                        logFile,
                        CREATE,
                        WRITE,
                        APPEND
                ),
                buffer
        );
        ByteBuffer indexBuf;
        if (indexWriter != null) {
            indexWriter.close();
            indexBuf = indexWriter.buffer;
        } else {
            indexBuf = ByteBuffer.allocateDirect(1 << 10);
        }
        Path indexFile = indexDir.resolve(currentDateFormat + ".idx");
        indexWriter = new BufferedFileChannel(
                FileChannel.open(
                        indexFile,
                        CREATE,
                        WRITE,
                        APPEND
                ),
                indexBuf
        );
        lastKeywordId = 0;
        dict.clear();
        writeMagicBlock();
    }

    @Override
    public void close() throws IOException {
        logWriter.close();
        indexWriter.close();
        logWriter = null;
    }

    private static class BufferedFileChannel {
        private final FileChannel channel;
        private ByteBuffer buffer;
        public long offset;

        private static final int DEFAULT_CAPACITY = 8 << 10;
        private static final int MAX_CAPACITY = 1 << 20;

        private BufferedFileChannel(FileChannel channel, int capacity) throws IOException {
            this.channel = channel;
            this.buffer = ByteBuffer.allocateDirect(capacity);
            this.offset = channel.size();
        }

        public BufferedFileChannel(FileChannel channel, ByteBuffer buffer) throws IOException {
            this.channel = channel;
            this.buffer = buffer;
            this.offset = channel.size();
        }

        private void ensureCapacity(int needed) throws IOException {
            if (buffer.remaining() >= needed) {
                return;
            }

            flush(false);
            if (buffer.remaining() >= needed) {
                return;
            }
            int newCapacity = Math.clamp(buffer.position() + needed, buffer.capacity() * 2, MAX_CAPACITY);
            if (newCapacity > MAX_CAPACITY) {
                throw new IOException("Buffer overflow. Consider increasing MAX_CAPACITY");
            }
            ByteBuffer newBuf = ByteBuffer.allocateDirect(newCapacity);
            buffer.flip();
            newBuf.put(buffer);
            buffer = newBuf;
        }

        public long writeAndGetOffset(int estimatedCapacity, Consumer<ByteBuffer> writer) throws IOException {
            ensureCapacity(estimatedCapacity);
            long pos = offset;
            int start = buffer.position();
            writer.accept(buffer);
            offset += buffer.position() - start;
            return pos;
        }

        public void flush(boolean sync) throws IOException {
            if (buffer.position() == 0) {
                return;
            }
            buffer.flip();
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            buffer.clear();

            if (sync) {
                channel.force(false);
            }
        }

        public void close() throws IOException {
            flush(true);
            channel.close();
        }
    }

    private void writeTime(long millis, ByteBuffer buffer) {
        long dayMillis = Math.floorMod(millis + zoneOffset, 86400000L);

        long hours = dayMillis / 3600000L;
        long minutes = (dayMillis / 60000L) % 60;

        buffer.put((byte) ('0' + hours / 10));
        buffer.put((byte) ('0' + hours % 10));
        buffer.put((byte) ':');
        buffer.put((byte) ('0' + minutes / 10));
        buffer.put((byte) ('0' + minutes % 10));
    }
    private void writeTime0(long millis, ByteBuffer buffer) {
        long time = millis + zoneOffset;

        long hours = (time % 86400000L) / 3600000L;
        long minutes = (time % 3600000L) / 60000L;

        if (hours >= 20) {
            buffer.put((byte) '2');
            buffer.put((byte) (hours - 20 + 48));
        } else if (hours >= 10) {
            buffer.put((byte) '1');
            buffer.put((byte) (hours - 10 + 48));
        } else {
            buffer.put((byte) '0');
            buffer.put((byte) (hours + 48));
        }
        buffer.put((byte) ':');
        if (minutes < 10) {
            buffer.put((byte) '0');
            buffer.put((byte) (minutes + 48));
        } else {
            buffer.put((byte) ((minutes / 10) + 48));
            buffer.put((byte) ((minutes % 10) + 48));
        }
    }
}