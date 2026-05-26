package dev.by1337.sync.bukkit.test;

import dev.by1337.core.BCore;
import dev.by1337.sync.client.channel.handler.lock.ClientLocksHandler;
import dev.by1337.sync.client.channel.handler.lock.LockManager;
import dev.by1337.sync.client.channel.handler.lock.Locks;
import net.kyori.adventure.text.Component;
import org.bukkit.Bukkit;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.HandlerList;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.event.player.PlayerQuitEvent;
import org.bukkit.inventory.Inventory;
import org.bukkit.inventory.ItemStack;
import org.bukkit.plugin.Plugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.UUID;

public class TestInvSync implements LockManager, Listener {
    private static final Logger log = LoggerFactory.getLogger(TestInvSync.class);
    private final ClientLocksHandler locks;
    private final Plugin plugin;

    public TestInvSync(ClientLocksHandler locks, Plugin plugin) {
        this.locks = locks;
        this.plugin = plugin;
        plugin.getServer().getPluginManager().registerEvents(this, plugin);
    }

    @Override
    public void close() {
        HandlerList.unregisterAll(this);
    }

    @Override
    public boolean ensureLockOwnership(UUID key) {
        return Bukkit.getPlayer(key) != null;
    }

    @Override
    public void acceptMail(UUID key, String json) {
        log.info("Mail {} for {}", json, key);
    }

    @Override
    public void forceUnlock(UUID key) {
        Player player = Bukkit.getPlayer(key);
        if (player != null) {
            var inv = player.getInventory();
            //  var data = saveInv(inv);
            inv.clear();
        }
    }

    @EventHandler
    public void on(PlayerJoinEvent event) {
        var uuid = event.getPlayer().getUniqueId();
        locks.lockAndLoadData(uuid, (status, payload) -> {
            var pl = Bukkit.getPlayer(uuid);
            if (status == Locks.LockStatus.FAILURE) {
                if (pl != null) {
                    Bukkit.getScheduler().runTask(plugin, () -> pl.kick(Component.text("Не удалось загрузить игровые данные!")));
                    log.error("Failed to load inventory!");
                }
            } else if (pl == null) {
                locks.unlock(uuid);
            } else if (payload != null) {
                loadInv(payload, pl.getInventory());
            }
        });
    }

    @EventHandler
    public void on(PlayerQuitEvent event) {
        var uuid = event.getPlayer().getUniqueId();
        if (locks.isLocked(uuid)) {
            var inv = event.getPlayer().getInventory();
            var data = saveInv(inv);
            inv.clear();
            locks.pushSnapshot(uuid, data);
            locks.unlock(uuid);
        }
        locks.pushMail(uuid, "TEST MAIL!");
    }

    private byte[] saveInv(Inventory inv) {
        try (var bytes = new ByteArrayOutputStream();
             var dos = new DataOutputStream(bytes);
        ) {
            int i = inv.getSize();
            dos.writeInt(i);
            for (int i1 = 0; i1 < i; i1++) {
                ItemStack item = inv.getItem(i1);
                if (item != null) {
                    dos.writeBoolean(true);
                    var arr = BCore.getItemStackSerializer().serialize(item, null);
                    dos.writeInt(arr.length);
                    dos.write(arr);
                } else {
                    dos.writeBoolean(false);
                }
            }
            dos.flush();
            return bytes.toByteArray();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private void loadInv(byte[] bytes, Inventory inv) {
        try (var dis = new DataInputStream(new ByteArrayInputStream(bytes))) {
            int i = dis.readInt();
            for (int i1 = 0; i1 < i; i1++) {
                if (dis.readBoolean()) {
                    byte[] item = new byte[dis.readInt()];
                    dis.readFully(item);
                    inv.setItem(i1, BCore.getItemStackSerializer().deserialize(item, null));
                }
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
