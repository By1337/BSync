package dev.by1337.sync.bukkit.test;

import dev.by1337.core.BCore;
import dev.by1337.sync.client.channel.handler.ClientLocksHandler;
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.UUID;

public class TestInvSyncHandler extends ClientLocksHandler implements Listener {
    private final Plugin plugin;

    public TestInvSyncHandler(Plugin plugin) {
        this.plugin = plugin;
        plugin.getServer().getPluginManager().registerEvents(this, plugin);
    }

    @Override
    public void close() {
        super.close();
        HandlerList.unregisterAll(this);
    }

    @Override
    protected byte[] forceUnlockNow(UUID key) {
        Player player = Bukkit.getPlayer(key);
        if (player == null){
            return null;
        }
        var inv = player.getInventory();
        var data = saveInv(inv);
        inv.clear();
        return data;
    }

    @Override
    protected void onMailAccept(UUID key, String json) {
        getLogger().info("Mail {} for {}", json, key);
    }

    @EventHandler
    public void on(PlayerJoinEvent event) {
        var uuid = event.getPlayer().getUniqueId();
        lockAndLoadData(uuid, (status, payload) -> {
            var pl = Bukkit.getPlayer(uuid);
            if (status == LockStatus.FAILURE) {
                if (pl != null) {
                    Bukkit.getScheduler().runTask(plugin, () -> pl.kick(Component.text("Не удалось загрузить игровые данные!")));
                    getLogger().error("Failed to load inventory!");
                }
            } else if (pl == null) {
                unlock(uuid, payload);
            }else if (payload != null){
                loadInv(payload, pl.getInventory());
            }
        });
    }

    @EventHandler
    public void on(PlayerQuitEvent event) {
        var uuid = event.getPlayer().getUniqueId();
        if (isLocked(uuid)) {
            var inv = event.getPlayer().getInventory();
            var data = saveInv(inv);
            inv.clear();
            unlock(uuid, data);
        }
        pushMail(uuid, "TEST MAIL!");
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
