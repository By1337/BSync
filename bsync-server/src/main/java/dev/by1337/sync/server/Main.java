package dev.by1337.sync.server;

import dev.by1337.sync.server.yaml.YamlReaderImpl;
import dev.by1337.yaml.YamlMap;
import org.slf4j.bridge.SLF4JBridgeHandler;

public class Main {


    public static void main(String[] args) throws Exception {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();

        var v = new DedicatedServer();
        v.readTerminal();
        //var pair =  Ed25519.generateKeyPair();
        // Files.writeString(Path.of("./test_ket.pub"), Ed25519.keyToBase64(pair.getPublic()));
        // Files.writeString(Path.of("./test_ket"), Ed25519.keyToBase64(pair.getPrivate()));
    }

    static {
        YamlMap.setYamlReader(new YamlReaderImpl());
    }
}
