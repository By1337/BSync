# BSync for Velocity

Java 21+, Velocity 3.4+ (API 3.4.0). Build from the BSync root: `mvn -pl bsync-velocity -am verify`. Put `bsync-velocity-1.6.jar` in the proxy plugins directory. The plugin ID is `bsync`; Bukkit/BLib are not required. BYaml is isolated inside the plugin. SnakeYAML and fastutil use Velocity’s copies (`provided` dependencies); neither library is bundled or relocated by BSync.

Configure `plugins/bsync/config.yml`. Its layout matches the Bukkit client: `id`, `workers`, `netty_threads`, and named `servers` entries with `group`, `ip`, `port`, `private_key`. The bundled config reads `{SERVER_ID}` and `{BSYNC_PRIVATE_KEY}` from the proxy process environment. Other `{ENV_NAME}` values work too, including host and port. Expansion happens before YAML parsing and does not escape values. Unknown variables remain unchanged; unresolved required identity/key values fail configuration validation.

Startup initiates connections asynchronously. An unavailable or restarting BSync backend does not shut down or block Velocity. The existing client reconnects and restores registered channels. Consumers must handle ChannelActiveMessage/ChannelInactiveMessage and restore their application subscriptions/snapshots when needed. Shutdown awaits connection closure. Runtime plugin reload/unload is not supported; worker lifetime follows the proxy process, as in the existing Bukkit adapter.

Consumer plugins declare `@Dependency(id = "bsync")` and use a provided Maven dependency `dev.by1337.sync:bsync-velocity:1.6`. Do not shade a second BSync client/common copy. After initialization, `dev.by1337.sync.velocity.BSync` exposes `getServerId()`, `getConnection(name)`, `getGroup(group)`, `connections()` and `workers()`. Connection/channel/publisher APIs are the existing BSync APIs; callbacks execute on BSync workers, not a Minecraft server thread.

This module provides transport. Server discovery, routing and gameplay teleportation belong to the network plugin, not this adapter.

## Runtime verification

`JAVA_HOME=/path/to/jdk mvn -pl bsync-velocity -am install` builds and installs local dependencies. Generate the fixture classpath with `mvn -pl bsync-velocity dependency:build-classpath -DincludeScope=test -Dmdep.outputFile=target/integration-classpath.txt`, then run `python3 bsync-velocity/tools/test-velocity.py /path/to/velocity.jar` from the BSync root. Requires Python with PyYAML and a built `bsync-server/target/bsync-server-1.6.jar`.

The test uses disposable directories, H2, generated keys and loopback ports. It checks startup while BSync is offline, environment identity, authenticated publisher round trips, inactive notifications, backend restart and restored channel delivery without restarting the proxy. Verified with an unmodified local Velocity 4.3.2-SNAPSHOT build.
