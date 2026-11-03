package dev.by1337.sync.k2v.standalone;

import dev.by1337.sync.k2v.PlayerDataRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.*;

class StandaloneRuntimeTest {
    @TempDir
    Path directory;

    @Test
    void fileRepositoryWorksWithoutAnyNetworkModule() throws Exception {
        run(false);
    }

    @Test
    void sqliteRepositoryWorksWithoutAnyNetworkModule() throws Exception {
        run(true);
    }

    private void run(boolean sqlite) throws Exception {
        String artifact = System.getProperty("bsync.test.k2vJar");
        URL main = artifact == null ? PlayerDataRepository.class.getProtectionDomain().getCodeSource().getLocation()
                : Path.of(artifact).toUri().toURL();
        URL[] paths = { main,
                getClass().getProtectionDomain().getCodeSource().getLocation() };
        try (var loader = new URLClassLoader(paths, getClass().getClassLoader()) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                if (name.startsWith("dev.by1337.sync.") && !name.startsWith("dev.by1337.sync.k2v."))
                    throw new ClassNotFoundException("Network module is forbidden: " + name);
                if (!name.startsWith("dev.by1337.sync.k2v.")) return super.loadClass(name, resolve);
                Class<?> result = findLoadedClass(name);
                if (result == null) result = findClass(name);
                if (resolve) resolveClass(result);
                return result;
            }
        }) {
            assertThrows(ClassNotFoundException.class, () -> loader.loadClass("dev.by1337.sync.common.mail.Mail"));
            assertThrows(ClassNotFoundException.class, () -> loader.loadClass("dev.by1337.sync.client.channel.ChannelMaker"));
            assertThrows(ClassNotFoundException.class, () -> loader.loadClass("dev.by1337.sync.bukkit.BSync"));
            Class<?> scenario = loader.loadClass(StandaloneScenario.class.getName());
            assertSame(loader, scenario.getClassLoader());
            scenario.getMethod("run", Path.class, boolean.class).invoke(null, directory, sqlite);
        }
    }
}
