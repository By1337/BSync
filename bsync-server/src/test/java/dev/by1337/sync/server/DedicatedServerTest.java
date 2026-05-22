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

public class DedicatedServerTest {


    @Test
    public void run() throws Exception {
        var server = new DedicatedServer();
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
       // CountDownLatch task = new CountDownLatch(1);
        SingleSemaphore task = new SingleSemaphore();
        locks.lockAndLoadData(new UUID(13, 37), (status, arr) -> {
            System.out.println(status);
            System.out.println(Arrays.toString(arr));
            locks.unlock(new UUID(13, 37), new byte[]{13, 37});
            locks.lockAndLoadData(new UUID(13, 37), (status1, arr1) -> {
                System.out.println(status1);
                System.out.println(Arrays.toString(arr1));
                task.release();
            });
        });
        System.out.println("done");
        while (!task.tryAcquire()){
        }
        // Thread.sleep(9999);
    }

    private static class TestClientLocks extends ClientLocksHandler {

        @Override
        protected byte[] forceUnlockNow(UUID key) {
            return new byte[]{13, 37};
        }

        @Override
        protected void onMailAccept(UUID key, String json) {
            System.out.println("Mail " + json);
        }
    }
}