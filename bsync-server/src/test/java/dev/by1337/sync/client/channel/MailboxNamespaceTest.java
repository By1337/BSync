package dev.by1337.sync.client.channel;

import dev.by1337.sync.common.mail.MailJournal;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class MailboxNamespaceTest {
    @TempDir
    Path root;

    @Test
    void shardsHaveDifferentOutboxesAndLegacyAdmissionIsMigratedWithoutChangingIds() {
        String client = "same-client", channel = "players";
        Path legacy = root.resolve(UUID.nameUUIDFromBytes((client + "\n" + channel).getBytes(StandardCharsets.UTF_8)).toString());
        UUID id;
        try (var journal = new MailJournal(legacy)) {
            id = journal.append(UUID.randomUUID(), "accepted earlier").id();
        }
        Path first = ClientChannel.resolveMailboxDirectory(root, client + "\nmaster-one:1010", client, channel);
        Path second = ClientChannel.resolveMailboxDirectory(root, client + "\nmaster-two:1010", client, channel);
        assertNotEquals(first, second);
        try (var one = new MailJournal(first); var two = new MailJournal(second); var old = new MailJournal(legacy)) {
            assertEquals(1, one.pending().size());
            assertEquals(id, one.pending().get(0).id());
            assertTrue(two.pending().isEmpty());
            assertTrue(old.pending().isEmpty());
        }
    }
}
