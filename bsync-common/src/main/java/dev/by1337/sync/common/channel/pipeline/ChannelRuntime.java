package dev.by1337.sync.common.channel.pipeline;

import dev.by1337.sync.common.work.EventLoopWorker;
import org.slf4j.Logger;

public interface ChannelRuntime {
    Pipeline pipeline();
    EventLoopWorker eventLoop();
    Logger logger();
}
