package dev.by1337.sync.common.channel.pipeline;

import dev.by1337.sync.common.channel.ChannelMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface ChannelHandler {
    Logger DEFAULT_LOGGER = LoggerFactory.getLogger(ChannelHandler.class);

    void init(ChannelRuntime runtime);

    void handle(ChannelContext ctx, ChannelMessage msg) throws Exception;

    void close();


}
