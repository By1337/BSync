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
    }

    static {
        YamlMap.setYamlReader(new YamlReaderImpl());
    }
}
