package dev.by1337.sync.server.console;

import dev.by1337.cmd.CommandReader;
import dev.by1337.sync.server.DedicatedServer;
import org.jline.reader.*;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class TerminalReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(TerminalReader.class);
    private final DedicatedServer server;
    private final CommandManager commandManager;

    public TerminalReader(DedicatedServer server, CommandManager commandManager) {
        this.server = server;
        this.commandManager = commandManager;
    }

    public void start() {
        try {
            Terminal terminal = TerminalBuilder.builder().encoding(StandardCharsets.UTF_8).dumb(true).build();
            this.readCommands(terminal);
        } catch (IOException e) {
            LOGGER.error("Error while reading commands", e);
        }
    }

    private void readCommands(Terminal terminal) {
        LineReader reader = this.buildReader(LineReaderBuilder.builder().terminal(terminal));
        try {
            while (server.isRunning()) {
                String line;
                try {
                    line = reader.readLine("> ");
                } catch (EndOfFileException e) {
                    continue;
                }

                if (line == null) {
                    break;
                }
                if (line.isBlank()) continue;

                this.processInput(line.trim());
            }
        } catch (UserInterruptException var10) {
            if (server.isRunning()) {
                server.shutdown();
            }
        }
    }

    protected void processInput(String input) {
        try {
            commandManager.getRootNode().execute(server, input);
        } catch (Exception e) {
            LOGGER.error("An error occurred while executing the command", e);
        }
    }

    protected LineReader buildReader(LineReaderBuilder builder) {
        builder
                .appName("BSync")
                .variable(LineReader.HISTORY_FILE, java.nio.file.Paths.get(".console_history"))
                .completer(new ConsoleCommandCompleter())
                .option(LineReader.Option.COMPLETE_IN_WORD, true);
        return builder.build();
    }

    public class ConsoleCommandCompleter implements Completer {

        @Override
        public void complete(LineReader lineReader, ParsedLine parsedLine, List<Candidate> list) {
            commandManager.getRootNode().suggest(server, prepareStringReader(parsedLine.line())).forEach(s -> {
                list.add(new Candidate(s));
            });
        }


        static CommandReader prepareStringReader(final String line) {
            final CommandReader reader = new CommandReader(line);
            if (reader.hasNext() && reader.peek() == '/') {
                reader.skip();
            }
            return reader;
        }
    }
}