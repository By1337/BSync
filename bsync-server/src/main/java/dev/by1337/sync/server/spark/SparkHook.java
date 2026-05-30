package dev.by1337.sync.server.spark;

import dev.by1337.cmd.*;
import dev.by1337.cmd.argument.ArgumentStrings;
import dev.by1337.sync.server.DedicatedServer;
import me.lucko.spark.common.SparkPlatform;
import me.lucko.spark.common.SparkPlugin;
import me.lucko.spark.common.command.sender.CommandSender;
import me.lucko.spark.common.monitor.ping.PlayerPingProvider;
import me.lucko.spark.common.platform.PlatformInfo;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.serializer.plain.PlainTextComponentSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;
import java.util.logging.Level;
import java.util.stream.Stream;

public class SparkHook implements SparkPlugin, CommandSender {
    private static final Logger log = LoggerFactory.getLogger(SparkHook.class);
    // private final ThreadDumper gameThreadDumper;
    private final SparkPlatform platform;
    private final DedicatedServer server;

    public SparkHook(DedicatedServer server) {
        this.server = server;
        platform = new SparkPlatform(this);
        server.commandManager().getRootNode().sub(new Command<DedicatedServer>("spark").executor(
                new Argument<DedicatedServer, String>("arg") {
                    @Override
                    public void parse(DedicatedServer ctx, CommandReader reader, ArgumentMap out) throws CommandMsgError {
                        out.put(name, readAll(reader));
                    }

                    @Override
                    public void suggest(DedicatedServer ctx, CommandReader reader, SuggestionsList suggestions, ArgumentMap args) throws CommandMsgError {
                        var s = readAll(reader);
                       // System.out.println(Arrays.toString(s.split(" ")));
                        var l = platform.tabCompleteCommand(SparkHook.this, s.split(" "));
                        for (String string : l) {
                            suggestions.suggest(string);
                        }
                    }
                    private String readAll(CommandReader reader){
                        String src = reader.src();
                        int idx = reader.ridx();
                        if (idx >= src.length()) {
                            return "";
                        }
                        var v = src.substring(idx);
                        reader.ridx(src.length());
                        return v;
                    }
                },
                (s, ar) -> {
                    platform.executeCommand(this, ar.split(" "));
                }
        ));
    }

    //    @Override
    //    public boolean onCommand(CommandSender sender, Command command, String label, String[] args) {
    //        this.platform.executeCommand(new BukkitCommandSender(sender, this.audienceFactory), args);
    //        return true;
    //    }
    //
    //    @Override
    //    public List<String> onTabComplete(CommandSender sender, Command command, String alias, String[] args) {
    //        return this.platform.tabCompleteCommand(new BukkitCommandSender(sender, this.audienceFactory), args);
    //    }

    @Override
    public String getVersion() {
        return "1.0";
    }

    @Override
    public Path getPluginDirectory() {
        return Path.of("./addons");
    }

    @Override
    public String getCommandName() {
        return "spark";
    }

    @Override
    public Stream<? extends CommandSender> getCommandSenders() {
        return Stream.empty();
    }

    @Override
    public void executeAsync(Runnable runnable) {
        ForkJoinPool.commonPool().execute(runnable);
    }

    @Override
    public void log(Level level, String s) {
        if (level == Level.SEVERE) {
            log.error(s);
        } else if (level == Level.WARNING) {
            log.warn(s);
        } else {
            log.info(s);
        }
    }

    @Override
    public void log(Level level, String s, Throwable throwable) {
        if (level == Level.SEVERE) {
            log.error(s, throwable);
        } else if (level == Level.WARNING) {
            log.warn(s, throwable);
        } else {
            log.info(s, throwable);
        }
    }

    @Override
    public PlayerPingProvider createPlayerPingProvider() {
        return new ConnectionPingProvider(server);
    }
    @Override
    public PlatformInfo getPlatformInfo() {
        return new PlatformInfo() {
            @Override
            public Type getType() {
                return Type.APPLICATION;
            }

            @Override
            public String getName() {
                return "BSync";
            }

            @Override
            public String getBrand() {
                return "";
            }

            @Override
            public String getVersion() {
                return "1.0";
            }

            @Override
            public String getMinecraftVersion() {
                return null;
            }
        };
    }

    @Override
    public String getName() {
        return "console";
    }

    @Override
    public UUID getUniqueId() {
        return new UUID(0, 0);
    }

    @Override
    public void sendMessage(Component component) {
        log.info(PlainTextComponentSerializer.plainText().serialize(component));
    }

    @Override
    public boolean hasPermission(String s) {
        return true;
    }
}
