package dev.by1337.sync.server.channel.handler.log;
import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.sql.Time;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class GenFile {

    static final int TARGET_SIZE = 1 << 30;
    static final Random RND = new Random(1);
    static final String[] array = new String[256];
    static String randomString() {
        return array[RND.nextInt(256)];
    }
    static String randomString0() {
        int len = 4 + RND.nextInt(28); // 4..31 bytes approx
        byte[] arr = new byte[len];
        for (int i = 0; i < len; i++) {
            arr[i] = (byte) ('a' + RND.nextInt(26));
        }
        return new String(arr);
    }

    public static void main(String[] args) throws Exception {
        for (int i = 0; i < array.length; i++) {
            array[i] = randomString0();
        }
        Path path = Paths.get("/home/by1337/tmp/test/data2.bin");
        long nanos;
        try (FileChannel ch = FileChannel.open(
                path,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            nanos = System.nanoTime();

            ByteBuffer buf = ByteBuffer.allocateDirect(8 << 20);

            int written = 0;

            while (written < TARGET_SIZE) {
                String s = randomString();
                byte[] str = s.getBytes();

                long payload = RND.nextLong();

                int recordSize = 4 + str.length + 8;

                if (buf.remaining() < 4 << 20) {
                    buf.flip();
                    while (buf.hasRemaining()) ch.write(buf);
                    buf.clear();
                }

                buf.putInt(str.length);
                buf.put(str);
                buf.putLong(payload);

                written += recordSize;
            }

            buf.flip();
            while (buf.hasRemaining()) ch.write(buf);
        }

        System.out.println("done " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - nanos));
    }
}