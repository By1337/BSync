package dev.by1337.sync.common.packet;

import dev.by1337.sync.common.callback.ResponseFuture;
import dev.by1337.sync.common.channel.ChannelMessage;
import dev.by1337.sync.common.channel.handler.request.RequestsHandler;
import dev.by1337.sync.common.channel.pipeline.Connection;
import dev.by1337.sync.common.channel.pipeline.Pipeline;
import io.netty.buffer.ByteBuf;

public interface Packet extends ChannelMessage {

    void write(ByteBuf buf, int protocolVersion);

    default ResponseFuture<Boolean> withAck(Pipeline pipeline, Connection connection){
        return withAck(pipeline, connection, 15_000);
    }
    default ResponseFuture<Boolean> withAck(Pipeline pipeline, Connection connection, long timeout){
        ResponseFuture<Boolean> future = new ResponseFuture<>();
        RequestsHandler.withAck(
                this,
                future,
                timeout
        ).execute(pipeline, connection);
        return future;
    }
}
