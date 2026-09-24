package dev.by1337.sync.server.channel.handler.lock;

import dev.by1337.sync.common.channel.pipeline.ChannelHandler;

import java.util.UUID;

public interface MailboxHandler extends ChannelHandler {
    void pushMail(UUID key, String json);
}
