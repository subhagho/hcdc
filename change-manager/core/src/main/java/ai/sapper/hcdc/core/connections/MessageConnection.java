package ai.sapper.hcdc.core.connections;

public interface MessageConnection extends Connection {
    boolean canSend();

    boolean canReceive();
}
