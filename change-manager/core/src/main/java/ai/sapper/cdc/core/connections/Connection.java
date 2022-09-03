package ai.sapper.cdc.core.connections;

import ai.sapper.cdc.common.AbstractState;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.Closeable;

public interface Connection extends Closeable {
    enum EConnectionState {
        Unknown, Initialized, Connected, Closed, Error
    }

    class ConnectionState extends AbstractState<EConnectionState> {

        public ConnectionState() {
            super(EConnectionState.Error);
            state(EConnectionState.Unknown);
        }

        public boolean isConnected() {
            return (state() == EConnectionState.Connected);
        }
    }

    String name();

    Connection init(@NonNull HierarchicalConfiguration<ImmutableNode> config) throws ConnectionError;
    Connection init(@NonNull String name,
                    @NonNull ZookeeperConnection connection,
                    @NonNull String path) throws ConnectionError;

    Connection connect() throws ConnectionError;

    Throwable error();

    EConnectionState connectionState();

    @JsonIgnore
    boolean isConnected();

    String path();

    Object settings();
}
