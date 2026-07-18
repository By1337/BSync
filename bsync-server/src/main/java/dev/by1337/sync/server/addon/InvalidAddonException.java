package dev.by1337.sync.server.addon;


public class InvalidAddonException extends Exception {
    public InvalidAddonException() {
    }

    public InvalidAddonException(String message) {
        super(message);
    }

    public InvalidAddonException(String message, Throwable cause) {
        super(message, cause);
    }


    public InvalidAddonException(Throwable cause) {
        super(cause);
    }

}
