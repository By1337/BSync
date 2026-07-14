/*
package dev.by1337.sync;

import com.destroystokyo.paper.profile.PlayerProfile;
import org.bukkit.event.player.AsyncPlayerPreLoginEvent;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.junit.Assert.*;

public class FilePlayerDataRepositoryTest {

   // @Test
    public void getUser() throws IOException {
        File testFolder = new File("./file_repo_test");
        if (testFolder.exists()){
            for (File file : testFolder.listFiles()) {
                file.delete();
            }
        }else {
            testFolder.mkdirs();
        }

        FilePlayerDataRepository<String> repo = new FilePlayerDataRepository<>(testFolder, null, new DataManager<>() {
            @Override
            public @NotNull String read(byte @Nullable [] data) {
                if (data == null) return "";
                return new String(data, StandardCharsets.UTF_8);
            }

            @Override
            public byte @NotNull [] write(@NotNull String data) {
                return data.getBytes(StandardCharsets.UTF_8);
            }

            @Override
            public void acceptMail(@NotNull String data, @NotNull String mail) {
            }

            @Override
            public void forceUnlock(UUID key) {
            }
        });
        UUID key = new UUID(13, 37);

        PlayerProfile profile = Mockito.mock(PlayerProfile.class);
        Mockito.when(profile.getId()).thenReturn(key);
        AsyncPlayerPreLoginEvent preLogin = new AsyncPlayerPreLoginEvent(
                "_By1337_",
                InetAddress.getLoopbackAddress(),
                InetAddress.getLoopbackAddress(),
                key,
                profile
        );
        repo.onLogin(preLogin);
        assertEquals("", repo.getUser(key));
        repo.pushSnapshot(key, "1337");
        assertEquals("1337", repo.getUser(key));
        repo.close();

        repo.onLogin(preLogin);
        assertEquals("1337", repo.getUser(key));
        repo.close();

        for (File file : testFolder.listFiles()) {
            file.delete();
        }
        testFolder.delete();
    }

}*/
