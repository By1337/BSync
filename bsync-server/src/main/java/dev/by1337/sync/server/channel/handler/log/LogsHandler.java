package dev.by1337.sync.server.channel.handler.log;

import dev.by1337.sync.common.channel.ChannelMessage;
import dev.by1337.sync.common.channel.pipeline.ChannelContext;
import dev.by1337.sync.common.channel.pipeline.ChannelHandler;
import dev.by1337.sync.common.channel.pipeline.ChannelRuntime;
import dev.by1337.sync.common.channel.pipeline.Pipeline;
import dev.by1337.sync.common.packet.impl.c2s.C2SWriteLogPacket;
import dev.by1337.sync.common.work.EventLoopWorker;
import dev.by1337.sync.server.DedicatedServer;
import dev.by1337.sync.server.channel.ServerChannelRuntime;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Path;

public class LogsHandler implements ChannelHandler {

    private Logger log = DEFAULT_LOGGER;
    private EventLoopWorker eventLoop;
    private Pipeline pipeline;
    private ServerChannelRuntime serverChannel;
    private boolean closing = false;
    private DedicatedServer server;
    private String id;
    private Path logsFolder;
    private LogStorage storage;

    @Override
    public void init(ChannelRuntime r) {
        if (!(r instanceof ServerChannelRuntime runtime))
            throw new IllegalArgumentException("runtime must be a ServerChannelRuntime");
        if (this.eventLoop != null) {
            throw new IllegalStateException("Duplicate handler add!");
        }
        id = runtime.name();
        logsFolder = Path.of("./" + id);
        try {
            storage = new LogStorage(logsFolder);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.eventLoop = runtime.eventLoop();
        this.log = runtime.logger();
        pipeline = runtime.pipeline();
        serverChannel = runtime;
        eventLoop.schedule(this::tick, 200);
        server = runtime.server();
    }

    private void tick() {
        if (closing) return;
        if (storage != null) {
            try {
                storage.flush();
            } catch (Exception e) {
                log.error("Failed to flush logs!", e);
            }
        }
        eventLoop.schedule(this::tick, 200);
    }

    @Override
    public void handle(ChannelContext ctx, ChannelMessage msg) {
        if (msg instanceof C2SWriteLogPacket packet) {
            try {
                storage.append(packet.timestamp(), packet.log(), packet.keywords());
            } catch (Exception e) {
                log.error("Failed to write log!", e);
            }
        } else {
            ctx.fire(msg);
        }
    }

    @Override
    public void close() {
        closing = true;
        var v = storage;
        storage = null;
        if (v != null) {
            try {
                v.close();
            } catch (Exception e) {
                log.error("Failed to flush logs!", e);
            }
        }
    }
}
