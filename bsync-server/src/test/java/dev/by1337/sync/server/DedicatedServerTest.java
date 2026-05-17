package dev.by1337.sync.server;

import dev.by1337.sync.client.config.ConnectionConfig;
import dev.by1337.sync.client.network.ClientBootstrap;
import dev.by1337.sync.client.network.Connection;
import dev.by1337.sync.client.work.ConnectionWorker;
import org.junit.Test;

public class DedicatedServerTest {


    @Test
    public void run() throws Exception {
        //   System.out.println(new File("authorized_keys").listFiles().length);
       var server = new DedicatedServer();

        Connection connection = new Connection(
                new ConnectionWorker("test-worker") ,
                new ConnectionConfig("test",
                        "localhost",
                        server.config().tcp_port,
                        "./authorized_keys/test_key"
                ),
                "test",
                new ClientBootstrap()
        );
        connection.connect();
        while (!connection.hasConnection()){
        }
        System.out.println("done");
        Thread.sleep(9999);
    }

}