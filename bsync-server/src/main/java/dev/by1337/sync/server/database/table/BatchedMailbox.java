package dev.by1337.sync.server.database.table;

import dev.by1337.sync.common.work.EventLoopWorker;

import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

public class BatchedMailbox {
    private final MailboxRepository mailbox;
    private final DataBatcher<MailboxRepository.Mail> addBatcher;
    private final DataBatcher<MailboxRepository.Mail> removeBatcher;

    public BatchedMailbox(MailboxRepository mailbox, EventLoopWorker worker) {
        this.mailbox = mailbox;
        var v = DataBatcher.createPair(
                2048,
                worker,
                mailbox::putAll,
                mailbox::removeAll
        );
        addBatcher = v.first();
        removeBatcher = v.second();
    }

    public void addMail(MailboxRepository.Mail mail){
        addBatcher.offer(mail);
    }

    public void removeMail(MailboxRepository.Mail mail){
        removeBatcher.offer(mail);
    }


    public int getMaxId() throws SQLException {
        return mailbox.getMaxId();
    }

    public List<MailboxRepository.Mail> loadAll(UUID owner) throws SQLException {
        return mailbox.loadAll(owner);
    }

    public void close() throws SQLException {
        addBatcher.close();
        removeBatcher.close();
    }
}
