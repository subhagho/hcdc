package ai.sapper.hcdc.core.connections;

import ai.sapper.hcdc.common.AbstractState;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

public interface Connection {
    public static enum EConnectionState {
        Unknown, Initialized, Connected, Disconnected, Error
    }

    public static class ConnectionState extends AbstractState<EConnectionState> {

        public ConnectionState() {
            super(EConnectionState.Error);
        }

        public boolean isConnected() {
            return (state() == EConnectionState.Connected);
        }
    }

    Connection init(@NonNull XMLConfiguration config, String pathPrefix) throws ConnectionError;

    Connection connect() throws ConnectionError;

    Throwable error();

    EConnectionState state();

    HierarchicalConfiguration<ImmutableNode> config();

    EConnectionState close() throws ConnectionError;

}
