package dev.by1337.sync.server.addon;

import dev.by1337.sync.server.DedicatedServer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class AddonClassLoader extends URLClassLoader {
    private final AddonDescription description;
    private final AddonLoader loader;
    private final AbstractAddon addon;
    private final File file;
    private final JarFile jar;
    private final Map<String, Class<?>> classes = new ConcurrentHashMap<>();
    private final DedicatedServer server;

    public AddonClassLoader(@Nullable ClassLoader parent, File file, AddonLoader loader, DedicatedServer server) throws Exception {
        super(new URL[]{file.toURI().toURL()}, parent);
        this.server = server;
        String fileName = file.getName();
        String version;
        if (fileName.contains("-")) {
            version = fileName.split("-", 2)[1].replace(".jar", "");
        } else {
            version = "1.0";
        }

        this.loader = loader;
        this.file = file;
        try {
            jar = new JarFile(file);
        } catch (IOException e) {
            throw new InvalidAddonException("Failed to open jar file: " + file.getPath(), e);
        }
        AddonDescription description = null;
        var iter = jar.entries().asIterator();
        while (iter.hasNext()) {
            var e = iter.next();
            if (!e.getName().endsWith(".class")) continue;
            String cl = e.getName().replace(".class", "").replace("/", ".");
            try {
                Class<?> clazz = Class.forName(cl, false, this);
                if (clazz.isAnnotationPresent(BSyncAddon.class)) {
                    BSyncAddon a = clazz.getAnnotation(BSyncAddon.class);
                    description = new AddonDescription(a.name(), cl, version, a.author());
                    break;
                }
            } catch (ClassNotFoundException ignored) {
            }
        }
        if (description == null){
            close();
            throw new InvalidAddonException("has no main class");
        }
        this.description = description;
        try {
            Class<?> jarClass;
            try {
                jarClass = Class.forName(description.mainClass(), true, this);
            } catch (ClassNotFoundException ex) {
                throw new InvalidAddonException("Cannot find main class "+description.mainClass(), ex);
            }
            Class<? extends AbstractAddon> pluginClass;
            try {
                pluginClass = jarClass.asSubclass(AbstractAddon.class);
            } catch (ClassCastException ex) {
                throw new InvalidAddonException("main class `" + description.mainClass() + "' does not extend AbstractAddon", ex);
            }
            addon = pluginClass.newInstance();
        } catch (IllegalAccessException ex) {
            throw new InvalidAddonException("No public constructor", ex);
        } catch (InstantiationException ex) {
            throw new InvalidAddonException("Abnormal service type", ex);
        }
    }

    public AbstractAddon addon() {
        return addon;
    }

    void initialize(AbstractAddon addon) {
        addon.init(LoggerFactory.getLogger(description.name()), description, this, file, server);
    }

    @Override
    public URL getResource(String name) {
        return findResource(name);
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
        return findResources(name);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        return loadClass0(name, resolve, true);
    }

    Class<?> loadClass0(@NotNull String name, boolean resolve, boolean global) throws ClassNotFoundException {
        try {
            return super.loadClass(name, resolve);
        } catch (ClassNotFoundException ignore) {
        }

        if (global) {
            Class<?> result = loader.getClassByName(name, resolve);
            if (result != null) {
                return result;
            }
        }
        throw new ClassNotFoundException(name);
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        Class<?> result = this.classes.get(name);
        if (result != null) return result;
        String path = name.replace('.', '/').concat(".class");
        JarEntry entry = this.jar.getJarEntry(path);
        if (entry != null) {
            byte[] classBytes;
            try (var in = jar.getInputStream(entry)) {
                classBytes = in.readAllBytes();
            } catch (IOException ex) {
                throw new ClassNotFoundException(name, ex);
            }
            //classBytes = Bukkit.getServer().getUnsafe().processClass(plugin.getDescription(), path, classBytes);
            result = defineClass(name, classBytes, 0, classBytes.length);
            classes.put(name, result);
            return result;
        }
        return super.findClass(name);
    }

    public AddonDescription description() {
        return description;
    }

    public File file() {
        return file;
    }

    public JarFile jar() {
        return jar;
    }

    @Override
    public void close() throws IOException {
        try {
            super.close();
        } finally {
            jar.close();
        }
    }
}
