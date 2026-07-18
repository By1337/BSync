package dev.by1337.sync.server.addon;

import dev.by1337.sync.server.DedicatedServer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.Objects;

public abstract class AbstractAddon {
    private Logger logger;
    private boolean isEnabled = false;
    private AddonDescription description;
    private ClassLoader classLoader;
    private File file;
    private DedicatedServer server;

    public AbstractAddon() {
        ClassLoader classLoader = this.getClass().getClassLoader();
        if (classLoader instanceof AddonClassLoader loader) {
            loader.initialize(this);
        } else {
            throw new IllegalArgumentException("AbstractAddon requires " + AddonClassLoader.class.getName());
        }
    }

    void init(Logger logger, AddonDescription description, ClassLoader classLoader, File file, DedicatedServer server) {
        this.logger = logger;
        this.description = description;
        this.classLoader = classLoader;
        this.file = file;
        this.server = server;
    }

    protected abstract void onEnable();

    protected abstract void onDisable();

    public void setEnabled(boolean enabled) {
        if (enabled == isEnabled) return;
        this.isEnabled = enabled;
        if (enabled) {
            logger.info("enabling...");
            onEnable();
        } else {
            logger.info("disabling...");
            onDisable();
        }
    }

    public Logger getLogger() {
        return logger;
    }

    public boolean isEnabled() {
        return isEnabled;
    }

    public AddonDescription getDescription() {
        return description;
    }

    public DedicatedServer getServer() {
        return server;
    }

    public @Nullable InputStream getResource(@NotNull String filename) {
        try {
            URL url = classLoader.getResource(filename);
            if (url == null) {
                return null;
            } else {
                URLConnection connection = url.openConnection();
                connection.setUseCaches(false);
                return connection.getInputStream();
            }
        } catch (IOException var4) {
            return null;
        }
    }

    public void saveResourceToFile(String resource, File outputFile) {
        if (outputFile.exists()) return;
        outputFile.getParentFile().mkdirs();

        try (var in = Objects.requireNonNull(getResource(resource), "Resource " + resource + " not found!")) {
            try (var out = new FileOutputStream(outputFile)) {
                byte[] buffer = new byte[1024];
                int length;
                while ((length = in.read(buffer)) > 0) {
                    out.write(buffer, 0, length);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
