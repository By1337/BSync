package dev.by1337.sync.velocity.integration;

import com.google.inject.Inject;
import com.velocitypowered.api.event.Subscribe;
import com.velocitypowered.api.event.proxy.ProxyInitializeEvent;
import com.velocitypowered.api.event.proxy.ProxyShutdownEvent;
import com.velocitypowered.api.plugin.annotation.DataDirectory;
import com.velocitypowered.api.proxy.ProxyServer;
import dev.by1337.sync.velocity.BSync;
import dev.by1337.sync.client.channel.ChannelMaker;
import dev.by1337.sync.client.channel.status.ChannelActiveMessage;
import dev.by1337.sync.client.channel.status.ChannelInactiveMessage;
import dev.by1337.sync.client.network.ClientBootstrap;
import dev.by1337.sync.client.network.Connection;
import dev.by1337.sync.common.channel.ChannelType;
import dev.by1337.sync.common.channel.ChannelMessage;
import dev.by1337.sync.common.channel.pipeline.ChannelContext;
import dev.by1337.sync.common.channel.pipeline.ChannelHandler;
import dev.by1337.sync.common.packet.Packets;
import dev.by1337.sync.common.packet.impl.a2a.PublishPacket;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public final class Probe {
    private final ProxyServer proxy;
    private final Path directory;
    private Connection peer;
    private final AtomicInteger generation = new AtomicInteger();

    @Inject
    public Probe(ProxyServer proxy, @DataDirectory Path directory) {
        this.proxy = proxy;
        this.directory = directory;
    }

    @Subscribe
    public void initialize(ProxyInitializeEvent event) throws Exception {
        Files.createDirectories(directory);
        if (!BSync.getServerId().equals("velocity-env-probe")) throw new AssertionError("Environment identity was not expanded");
        if (BSync.getGroup("network").size() != 1) throw new AssertionError("Group lookup failed");
        var main = BSync.getConnection("main");
        var channel = main.addChannel("velocity-probe", ChannelType.PUBLISHER, cc -> {
            cc.addRegistries(Packets.BSYNC_PUBLISH);
            cc.pipeline().addLast("probe", new ChannelHandler() {
                @Override
                public void init(dev.by1337.sync.common.channel.pipeline.ChannelRuntime runtime) {
                }

                @Override
                public void close() {
                }

                @Override
                public void handle(ChannelContext ctx, ChannelMessage message) {
                    if (message instanceof ChannelActiveMessage) generation.incrementAndGet();
                    else if (message instanceof ChannelInactiveMessage) mark("inactive");
                    else if (message instanceof PublishPacket packet) {
                        String value = new String(packet.payload(), StandardCharsets.UTF_8);
                        if (value.equals("round:" + generation.get())) mark("round-" + generation.get());
                        else throw new AssertionError("Unexpected echo");
                    } else ctx.fire(message);
                }
            });
        });
        peer = new Connection(main.config(), BSync.workers(), "velocity-echo-peer", new ClientBootstrap());
        var echo = new AtomicReference<Consumer<byte[]>>();
        echo.set(ChannelMaker.createPublisher(peer, "velocity-probe", bytes -> echo.get().accept(bytes)));
        peer.connect();
        proxy.getScheduler().buildTask(this, () -> {
            if (generation.get() > 0 && main.hasConnection() && peer.hasConnection())
                channel.write(new PublishPacket(("round:" + generation.get()).getBytes(StandardCharsets.UTF_8)));
        }).repeat(200, TimeUnit.MILLISECONDS).schedule();
        mark("initialized");
    }

    private void mark(String name) {
        try {
            Files.writeString(directory.resolve(name), "PASS\n");
        } catch (Exception error) {
            throw new RuntimeException(error);
        }
    }

    @Subscribe
    public void shutdown(ProxyShutdownEvent event) {
        if (peer != null) peer.close();
    }
}
