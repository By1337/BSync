package dev.by1337.sync.common.channel;

import dev.by1337.sync.common.channel.pipeline.Connection;
import dev.by1337.sync.common.channel.pipeline.Pipeline;

public interface ChannelMessage {

    default void execute(Pipeline pipeline, Connection connection){
        pipeline.execute(this, connection);
    }

    interface UnhandledIgnored extends ChannelMessage {

    }
}
