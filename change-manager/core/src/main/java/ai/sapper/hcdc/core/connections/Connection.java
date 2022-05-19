package ai.sapper.hcdc.core.connections;

import ai.sapper.hcdc.common.AbstractState;
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

    Connection connect() throws ConnectionError;

    Throwable error();

    EConnectionState connectionState();

    boolean isConnected();

    HierarchicalConfiguration<ImmutableNode> config();

}
