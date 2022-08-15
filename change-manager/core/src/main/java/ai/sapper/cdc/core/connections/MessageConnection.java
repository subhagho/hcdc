package ai.sapper.cdc.core.connections;

public interface MessageConnection extends Connection {
    boolean canSend();

    boolean canReceive();
}
