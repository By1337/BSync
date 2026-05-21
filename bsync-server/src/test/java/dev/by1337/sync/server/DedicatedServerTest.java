package dev.by1337.sync.server;

import dev.by1337.sync.client.config.ConnectionConfig;
import dev.by1337.sync.client.network.ClientBootstrap;
import dev.by1337.sync.client.network.Connection;
import dev.by1337.sync.common.channel.ChannelType;
import dev.by1337.sync.common.work.EventLoopWorker;
import dev.by1337.sync.common.work.EventLoopWorkers;
import org.junit.Test;

public class DedicatedServerTest {


    @Test
    public void run() throws Exception {
        //   System.out.println(new File("authorized_keys").listFiles().length);
       var server = new DedicatedServer();
        EventLoopWorkers workers = new EventLoopWorkers("test-worker-%d", 1);
        Connection connection = new Connection(
                new ConnectionConfig("test",
                        "localhost",
                        server.config().tcp_port,
                        "./authorized_keys/test_key"
                ),
                workers,
                "test",
                new ClientBootstrap()
        );
        connection.connect();
        while (!connection.hasConnection()){
        }
        connection.addChannel("test-locks", ChannelType.DATA_CHANNEL, channel -> {
            channel.pipeline().addLast("locks", null);//todo
        });
        System.out.println("done");
        Thread.sleep(9999);
    }

}