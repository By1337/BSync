package dev.by1337.sync.server;

import dev.by1337.sync.client.channel.handler.ClientLocksHandler;
import dev.by1337.sync.client.config.ConnectionConfig;
import dev.by1337.sync.client.network.ClientBootstrap;
import dev.by1337.sync.client.network.Connection;
import dev.by1337.sync.common.channel.ChannelType;
import dev.by1337.sync.common.util.SingleSemaphore;
import dev.by1337.sync.common.work.EventLoopWorkers;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.locks.LockSupport;

public class DedicatedServerTest {


   // @Test
    public void run() throws Exception {
        //todo test
        // ping 100
        // client lock -> unlock -> lock

      /*  var server = new DedicatedServer(8014);
        EventLoopWorkers workers = new EventLoopWorkers("test-worker-%d", 1);
        Connection connection = new Connection(
                new ConnectionConfig(
                        "localhost",
                        server.config().tcp_port,
                        new File("./authorized_keys/test_key")
                ),
                workers,
                "test",
                new ClientBootstrap()
        );
        connection.connect();
        while (!connection.hasConnection()) {
        }
        var locks = new TestClientLocks();
        var channel = connection.addChannel("test-locks", ChannelType.DATA_CHANNEL, channel2 -> {
            channel2.pipeline().addLast("locks", locks);
        });
        while (!channel.registered()) {
        }
        System.out.println("done");

        var key = new UUID(13, 37);
       // CountDownLatch task = new CountDownLatch(1);
        SingleSemaphore task = new SingleSemaphore();
        task.tryAcquire();
        channel.eventLoop().execute(() -> {
            locks.lockAndLoadData(key, (s, a) -> {
                System.out.println("V1 " + s);
            });
        });



        while (!task.tryAcquire()){
        }
         Thread.sleep(9999);*/
    }

}