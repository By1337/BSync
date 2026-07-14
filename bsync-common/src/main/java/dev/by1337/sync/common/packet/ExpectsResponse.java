package dev.by1337.sync.common.packet;

import dev.by1337.sync.common.callback.ResponseFuture;
import dev.by1337.sync.common.channel.ChannelMessage;
import dev.by1337.sync.common.channel.handler.request.RequestsHandler;
import dev.by1337.sync.common.channel.pipeline.Connection;
import dev.by1337.sync.common.channel.pipeline.Pipeline;

public interface ExpectsResponse<T extends ChannelMessage> extends ChannelMessage {

    default ResponseFuture<T> request(Pipeline pipeline) {
        return request(pipeline, pipeline.local());
    }

    default ResponseFuture<T> request(Pipeline pipeline, Connection connection) {
        return request(pipeline, connection, 5_000);
    }

    //@SuppressWarnings("unchecked")
    //<V extends ExpectsResponse<T> & Packet>
    default ResponseFuture<T> request(Pipeline pipeline, Connection connection, long timeout) {
        //  if (this instanceof Packet) {
        ResponseFuture<T> future = new ResponseFuture<>();
        RequestsHandler.request(
                /*(V)*/ this,
                future,
                timeout
        ).execute(pipeline, connection);
        return future;
        // }
        // throw new IllegalStateException(getClass() + " is not a Packet!");
    }
}
