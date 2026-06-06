package dev.by1337.sync.server.channel.handler.log;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class LogGenerator {

    private static final String[] NICKNAME_PARTS = {
            "player", "user", "gamer", "pro", "noob", "killer", "master",
            "lord", "night", "dark", "fire", "ice", "storm", "thunder",
            "warrior", "mage", "hunter", "rogue", "paladin", "druid", "by1337"
    };

    private static final String[] ACTIONS = {
            "pay ${num} to {string}",
            "buy {string} for ${num}",
            "sell {string} for ${num}",
            "kill {string}",
            "join the game",
            "leave the game",
            "send message: '{string}'",
            "get achievement: {string}",
            "level up to {num}",
            "craft {string}",
            "mine {num} {string} ore",
            "defeat {string}"
    };

    private static final String[] ITEMS = {
            "diamond sword", "iron pickaxe", "golden apple", "potion", "bow",
            "arrow", "shield", "helmet", "chestplate", "leggings", "boots"
    };

    private static final String[] MESSAGES = {
            "gg", "wp", "hello", "good game", "nice!", "lol", "brb", "afk"
    };

    private static final String[] MOBS = {
            "zombie", "skeleton", "spider", "creeper", "ender dragon", "wither"
    };

    private static final String[] CATEGORIES = {
            "economy", "combat", "chat", "achievement", "system", "trade", "quest"
    };

    public static void main(String[] args) throws IOException {
        for (int i = 0; i < 40; i++) {
            main0();
        }
    }
    public static void main0() throws IOException {
        // Path logDir = Path.of("/home/by1337/tmp/test/");
        Path logDir = Path.of("/home/by1337/tmp/test/");

        // Создаём директорию если не существует
        Files.createDirectories(logDir);

        LogStorage logStorage = new LogStorage(logDir);

        Random random = new Random(42); // Фиксированный seed для воспроизводимости

        System.out.println("Generating 1,000,000 log entries...");
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < 1_000_000; i++) {

            var v = generateRandomNickname(random).toLowerCase();
            var v1 = generateRandomNickname(random).toLowerCase();
            logStorage.append(System.currentTimeMillis(),
                    ("some " + v + " to " + v1), v, v1
                    );

            if ((i + 1) % 100_000 == 0) {
                System.out.printf("Progress: %d/%d (%.1f%%)%n",
                        i + 1, 1_000_000, (i + 1) / 10_000.0);
            }
        }

        logStorage.close();
        long endTime = System.currentTimeMillis();
        System.out.printf("Done! Generated 1,000,000+3 entries in %d ms%n", (endTime - startTime));
        System.out.println("Logs saved to: " + logDir);
    }

    static int id = 0;
    private static String generateRandomNickname(Random random) {
        if (random.nextInt(5) == 0){
            return NICKNAME_PARTS[random.nextInt(NICKNAME_PARTS.length)];
        }
        return Integer.toString(id++);
    }

    private static String generateRandomMessage(Random random) {
      return   NICKNAME_PARTS[random.nextInt(NICKNAME_PARTS.length)];
    }

    private static String[] generateExtraParams(Random random) {
        // 70% - нет параметров, 20% - 1 параметр, 10% - 2 параметра
        int numParams;
        int r = random.nextInt(100);
        if (r < 70) {
            numParams = 0;
        } else if (r < 90) {
            numParams = 1;
        } else {
            numParams = 2;
        }

        String[] params = new String[numParams];
        for (int i = 0; i < numParams; i++) {
            if (random.nextBoolean()) {
                params[i] = ITEMS[random.nextInt(ITEMS.length)];
            } else {
                params[i] = NICKNAME_PARTS[random.nextInt(NICKNAME_PARTS.length)] +
                        (random.nextInt(1000) > 0 ? random.nextInt(1000) : "");
            }
        }
        return params;
    }
}