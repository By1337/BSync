package dev.by1337.sync.k2v.login;

import org.junit.jupiter.api.Test;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Supply -Dbsync.test.paperApi=/path/to/paper-api.jar to check the real runtime API. */
class ConfigurationConnectionApiTest {
    @Test
    void publicConnectionStatusWorksWithTheActualPaperApiJar() throws Exception {
        String jar = System.getProperty("bsync.test.paperApi");
        assumeTrue(jar != null, "Requires an external Paper API jar");
        String interfaceName = "io.papermc.paper.connection.PlayerConnection";
        // Load this interface from the supplied runtime jar rather than the merged
        // compile-time API, which intentionally contains several Paper versions.
        try (var loader = new URLClassLoader(new URL[]{ Path.of(jar).toUri().toURL() }, getClass().getClassLoader()) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                if (!name.equals(interfaceName)) return super.loadClass(name, resolve);
                Class<?> result = findLoadedClass(name);
                if (result == null) result = findClass(name);
                if (resolve) resolveClass(result);
                return result;
            }
        }) {
            Class<?> api = loader.loadClass(interfaceName);
            assertEquals(boolean.class, api.getMethod("isConnected").getReturnType());
            var active = new AtomicBoolean(true);
            Object connection = Proxy.newProxyInstance(loader, new Class<?>[]{ api }, (proxy, method, args) -> {
                if (method.getName().equals("isConnected")) return active.get();
                throw new UnsupportedOperationException(method.getName());
            });
            var connected = ConfigurationPacketLoginHook.configurationConnected(connection);
            assertTrue(connected.getAsBoolean());
            active.set(false);
            assertFalse(connected.getAsBoolean());
        }
    }
}
