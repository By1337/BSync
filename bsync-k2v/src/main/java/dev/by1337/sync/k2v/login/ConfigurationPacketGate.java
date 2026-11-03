package dev.by1337.sync.k2v.login;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.function.Supplier;

/** Holds the terminal configuration packet until the repository has loaded the player. */
public final class ConfigurationPacketGate extends ChannelInboundHandlerAdapter {
    private final Predicate<Object> terminalPacket;
    private final Supplier<CompletionStage<Boolean>> load;
    private final Queue<Object> held = new ArrayDeque<>();
    private boolean waiting;
    private boolean finished;

    public ConfigurationPacketGate(Predicate<Object> terminalPacket, Supplier<CompletionStage<Boolean>> load) {
        this.terminalPacket = terminalPacket;
        this.load = load;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object packet) {
        if (finished) {
            ctx.fireChannelRead(packet);
        } else if (waiting || terminalPacket.test(packet)) {
            if (held.size() >= 256) {
                ReferenceCountUtil.release(packet);
                discard();
                ctx.close();
                return;
            }
            held.offer(packet);
            if (!waiting) {
                waiting = true;
                try {
                    load.get().whenComplete((success, error) -> ctx.executor().execute(() -> {
                        if (finished) return;
                        if (error != null || !Boolean.TRUE.equals(success) || !ctx.channel().isActive()) {
                            discard();
                            ctx.close();
                            return;
                        }
                        finished = true;
                        Object next;
                        while ((next = held.poll()) != null) ctx.fireChannelRead(next);
                        ctx.fireChannelReadComplete();
                        if (ctx.pipeline().context(this) != null) ctx.pipeline().remove(this);
                    }));
                } catch (Exception e) {
                    discard();
                    ctx.close();
                }
            }
        } else {
            ctx.fireChannelRead(packet);
        }
    }

    private void discard() {
        finished = true;
        Object packet;
        while ((packet = held.poll()) != null) ReferenceCountUtil.release(packet);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        discard();
        ctx.fireChannelInactive();
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        if (waiting && !finished) {
            discard();
            ctx.close();
        }
    }
}
