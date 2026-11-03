package dev.by1337.sync.k2v.mail;

import org.junit.jupiter.api.Test;
import java.nio.file.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

public class MailJournalTest {
    @Test
    public void readsExistingCommonJournalAndKeepsItsFileFormat() throws Exception {
        Path directory = Files.createTempDirectory("k2v-journal-compatibility");
        UUID owner = UUID.randomUUID();
        dev.by1337.sync.common.mail.Mail previous;
        try (var legacy = new dev.by1337.sync.common.mail.MailJournal(directory)) {
            previous = legacy.append(owner, "pending before upgrade");
        }
        Mail next;
        try (var journal = new MailJournal(directory)) {
            Mail recovered = journal.pending().get(0);
            assertEquals(previous.id(), recovered.id());
            assertEquals(previous.owner(), recovered.owner());
            assertEquals(previous.payload(), recovered.payload());
            journal.acknowledge(recovered.id());
            next = journal.append(owner, "new pending mail");
        }
        try (var legacy = new dev.by1337.sync.common.mail.MailJournal(directory)) {
            assertEquals(1, legacy.pending().size());
            assertEquals(next.id(), legacy.pending().get(0).id());
            assertEquals(next.payload(), legacy.pending().get(0).payload());
        }
    }

    @Test
    public void replaysUnconfirmedRecordsWithStableIdsAndProducerOrder() throws Exception {
        Path directory = Files.createTempDirectory("bsync-journal-test");
        UUID owner = UUID.randomUUID();
        Mail first;
        Mail second;
        try (var journal = new MailJournal(directory)) {
            first = journal.append(owner, "line one\nстрока два");
            second = journal.append(owner, "second");
        }
        try (var journal = new MailJournal(directory)) {
            assertEquals(journal.pending(), List.of(first, second));
            journal.acknowledge(first.id());
        }
        try (var journal = new MailJournal(directory)) {
            assertEquals(journal.pending(), List.of(second));
        }
    }

    @Test
    public void cannotOpenSameJournalTwiceOrAppendAfterClose() throws Exception {
        Path directory = Files.createTempDirectory("bsync-journal-lock-test");
        var journal = new MailJournal(directory);
        try {
            assertThrows(RuntimeException.class, () -> new MailJournal(directory));
        } finally {
            journal.close();
        }
        assertThrows(IllegalStateException.class, () -> journal.append(UUID.randomUUID(), "mail"));
        try (var reopened = new MailJournal(directory)) {
            assertTrue(reopened.pending().isEmpty());
        }
    }

    @Test
    public void orphanedTemporaryRecordDoesNotBlockFutureAdmissions() throws Exception {
        Path directory = Files.createTempDirectory("bsync-journal-tmp-test");
        Files.writeString(directory.resolve("0000000000000000001-" + UUID.randomUUID() + ".mail.tmp"), "partial");
        try (var journal = new MailJournal(directory)) {
            journal.append(UUID.randomUUID(), "mail");
            assertEquals(journal.pending().size(), 1);
        }
        try (var journal = new MailJournal(directory)) {
            assertEquals(journal.pending().size(), 1);
        }
    }

    @Test
    public void corruptionFailsStartupInsteadOfSilentlySkippingAnAcceptedMail() throws Exception {
        Path directory = Files.createTempDirectory("bsync-journal-corrupt-test");
        Files.writeString(directory.resolve("0000000000000000001-" + UUID.randomUUID() + ".mail"), "bad");
        assertThrows(RuntimeException.class, () -> new MailJournal(directory));
    }

    @Test
    public void acceptedRecordSurvivesAbruptProcessExitWithoutClose() throws Exception {
        Path directory = Files.createTempDirectory("bsync-journal-crash-test");
        String executable = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        Process process = new ProcessBuilder(executable, "-cp", System.getProperty("java.class.path"),
                CrashWriter.class.getName(), directory.toString()).redirectErrorStream(true).start();
        try {
            assertTrue(process.waitFor(10, java.util.concurrent.TimeUnit.SECONDS));
            assertEquals(process.exitValue(), 0);
            UUID id = UUID.fromString(new String(process.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).trim());
            try (var journal = new MailJournal(directory)) {
                assertEquals(journal.pending().size(), 1);
                assertEquals(journal.pending().get(0).id(), id);
                assertEquals(journal.pending().get(0).payload(), "accepted before crash");
            }
        } finally {
            process.destroyForcibly();
        }
    }

    public static class CrashWriter {
        public static void main(String[] args) {
            var journal = new MailJournal(Path.of(args[0]));
            var mail = journal.append(UUID.randomUUID(), "accepted before crash");
            System.out.println(mail.id());
            System.out.flush();
            Runtime.getRuntime().halt(0);
        }
    }

}
