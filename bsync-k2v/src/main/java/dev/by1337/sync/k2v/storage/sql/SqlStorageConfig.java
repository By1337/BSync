package dev.by1337.sync.k2v.storage.sql;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;

/** Credentials are deliberately excluded from toString and validation failures. */
public final class SqlStorageConfig {
    public final String scheme, host, database, repository, user, password;
    public final int port, poolSize;
    public final long leaseMillis, renewMillis, pollMillis, acquireWaitMillis, timeoutMillis;
    private final Properties driverProperties;

    private SqlStorageConfig(URI uri, Map<String, String> params, Path baseDirectory) {
        scheme = uri.getScheme().toLowerCase(Locale.ROOT);
        if (!scheme.equals("mysql") && !scheme.equals("mariadb") && !isSqlite()) throw invalid("scheme");
        if (uri.getFragment() != null) throw invalid("fragment");
        if (isSqlite()) {
            if (params.containsKey("lease") || params.containsKey("renew")) throw invalid("SQLite has no lease renewal");
            // Two slashes introduce a relative file, three an absolute file.
            // Decode as a path, preserving literal '+' characters.
            String authority = uri.getRawAuthority();
            if (uri.isOpaque() || uri.getRawPath() == null || uri.getRawUserInfo() != null
                    || (authority != null && (authority.contains(":") || authority.contains("@"))))
                throw invalid("sqlite file");
            String file = decodeInfo((authority == null ? "" : authority) + uri.getRawPath());
            if (file.isBlank() || file.endsWith("/") || file.indexOf('\0') >= 0) throw invalid("sqlite file");
            Path path = baseDirectory.resolve(file).toAbsolutePath().normalize();
            if (path.getFileName() == null || path.getFileName().toString().equals(":memory:")) throw invalid("sqlite file");
            database = path.toString();
            repository = params.getOrDefault("repository", "players");
            params.remove("repository");
            if (!repository.matches("[A-Za-z0-9_.-]{1,96}")) throw invalid("repository");
            host = "";
            port = 0;
            user = "";
            password = "";
        } else {
            host = uri.getHost();
            if (host == null || host.isBlank() || uri.getFragment() != null) throw invalid("host/fragment");
            port = uri.getPort() == -1 ? 3306 : uri.getPort();
            if (port < 1 || port > 65535) throw invalid("port");
            String[] path = uri.getRawPath().split("/", -1);
            if (path.length != 3) throw invalid("path: expected /database/repository");
            database = decode(path[1]);
            repository = decode(path[2]);
            if (!database.matches("[A-Za-z0-9_-]{1,64}") || !repository.matches("[A-Za-z0-9_.-]{1,96}")) throw invalid("database/repository");
            String userInfo = uri.getRawUserInfo();
            if (userInfo != null) {
                if (params.containsKey("user") || params.containsKey("password")) throw invalid("duplicate credentials");
                int colon = userInfo.indexOf(':');
                user = decodeInfo(colon < 0 ? userInfo : userInfo.substring(0, colon));
                password = colon < 0 ? "" : decodeInfo(userInfo.substring(colon + 1));
            } else {
                user = params.remove("user");
                password = params.getOrDefault("password", "");
                params.remove("password");
            }
            if (user == null || user.isBlank()) throw invalid("user");
        }
        leaseMillis = duration(params.remove("lease"), 30_000);
        renewMillis = duration(params.remove("renew"), leaseMillis - 3_000);
        timeoutMillis = duration(params.remove("timeout"), 1_000);
        pollMillis = duration(params.remove("poll"), 5_000);
        acquireWaitMillis = duration(params.remove("acquireWait"), 5_000);
        poolSize = integer(params.remove("pool"), isSqlite() ? 1 : 4);
        if ((!isSqlite() && (leaseMillis < 5_000 || leaseMillis > 300_000 || renewMillis < 1_000
                || renewMillis >= leaseMillis || leaseMillis - renewMillis < timeoutMillis * 2 + 500))
                || timeoutMillis < 250 || timeoutMillis > 10_000 || pollMillis < 1_000 || pollMillis > 60_000
                || acquireWaitMillis < 0 || acquireWaitMillis > 20_000 || poolSize < 1 || poolSize > 32)
            throw invalid("lease/renew/timeout/poll/acquireWait/pool");
        driverProperties = new Properties();
        if (isSqlite()) {
            if (!params.isEmpty()) throw invalid("unknown sqlite option");
            driverProperties.setProperty("journal_mode", "WAL");
            driverProperties.setProperty("synchronous", "FULL");
            driverProperties.setProperty("busy_timeout", Long.toString(timeoutMillis));
            driverProperties.setProperty("transaction_mode", "IMMEDIATE");
            return;
        }
        Set<String> allowed = Set.of("sslMode", "useSsl", "trustServerCertificate", "allowPublicKeyRetrieval",
                "enabledSslProtocolSuites", "serverSslCert", "connectionTimeZone", "serverTimezone");
        for (var entry : params.entrySet()) {
            if (!allowed.contains(entry.getKey())) throw invalid("unknown option");
            driverProperties.setProperty(entry.getKey(), entry.getValue());
        }
        driverProperties.setProperty("user", user);
        driverProperties.setProperty("password", password);
        driverProperties.setProperty("useAffectedRows", "false");
        driverProperties.setProperty("connectTimeout", Long.toString(timeoutMillis));
        driverProperties.setProperty("socketTimeout", Long.toString(timeoutMillis));
    }

    public static SqlStorageConfig parse(String text) {
        return parse(text, Path.of(""));
    }

    /** Resolve relative SQLite file paths against the supplied directory. */
    public static SqlStorageConfig parse(String text, Path baseDirectory) {
        try {
            URI uri = URI.create(text);
            Map<String, String> parameters = new LinkedHashMap<>();
            if (uri.getRawQuery() != null && !uri.getRawQuery().isEmpty()) {
                for (String pair : uri.getRawQuery().split("&", -1)) {
                    int equals = pair.indexOf('=');
                    if (equals < 1 || parameters.putIfAbsent(decode(pair.substring(0, equals)), decode(pair.substring(equals + 1))) != null)
                        throw invalid("duplicate or malformed option");
                }
            }
            return new SqlStorageConfig(uri, parameters, baseDirectory);
        } catch (Exception e) {
            // URI exceptions contain the complete input, including passwords. Do not retain the cause.
            throw new IllegalArgumentException("Invalid SQL storage URI; expected mysql://host:port/database/repository?user=...&password=... or sqlite:///path/file.db?repository=players");
        }
    }

    public String jdbcUrl() {
        if (isSqlite()) return "jdbc:sqlite:" + Path.of(database).toUri().toASCIIString();
        String address = host.contains(":") && !host.startsWith("[") ? "[" + host + "]" : host;
        return "jdbc:" + scheme + "://" + address + ":" + port + "/" + database;
    }

    public Properties driverProperties() {
        var result = new Properties();
        result.putAll(driverProperties);
        return result;
    }

    public String driverClass() {
        if (isSqlite()) return "org.sqlite.JDBC";
        return scheme.equals("mysql") ? "com.mysql.cj.jdbc.Driver" : "org.mariadb.jdbc.Driver";
    }

    /** Journal identity is stable across credential rotation and driver choice. */
    public String journalIdentity() {
        if (isSqlite()) return "sqlite:" + database + "/" + repository;
        return host + ":" + port + "/" + database + "/" + repository;
    }

    @Override
    public String toString() {
        if (isSqlite()) return "sqlite://" + Path.of(database).toUri().getRawPath() + "?repository=" + repository;
        return scheme + "://" + host + ":" + port + "/" + database + "/" + repository;
    }

    public boolean isSqlite() {
        return scheme.equals("sqlite");
    }

    private static String decode(String value) {
        return URLDecoder.decode(value, StandardCharsets.UTF_8);
    }

    private static String decodeInfo(String value) {
        return decode(value.replace("+", "%2B"));
    }

    private static int integer(String value, int fallback) {
        return value == null ? fallback : Integer.parseInt(value);
    }

    private static long duration(String value, long fallback) {
        if (value == null) return fallback;
        if (value.endsWith("ms")) return Long.parseLong(value.substring(0, value.length() - 2));
        if (value.endsWith("s")) return Math.multiplyExact(Long.parseLong(value.substring(0, value.length() - 1)), 1_000);
        throw invalid("duration: use ms or s");
    }

    private static IllegalArgumentException invalid(String field) {
        return new IllegalArgumentException("Invalid SQL storage " + field);
    }
}
