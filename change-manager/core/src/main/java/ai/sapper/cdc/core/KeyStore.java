package ai.sapper.cdc.core;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

@Getter
@Accessors(fluent = true)
public abstract class KeyStore {
    @Getter(AccessLevel.NONE)
    private String password;

    public KeyStore withPassword(@NonNull String password) {
        this.password = password;
        return this;
    }

    public void init(@NonNull HierarchicalConfiguration<ImmutableNode> configNode) throws ConfigurationException {
        init(configNode, password);
    }

    public abstract void init(@NonNull HierarchicalConfiguration<ImmutableNode> configNode,
                              @NonNull String password) throws ConfigurationException;

    public void save(@NonNull String name,
                     @NonNull String value) throws Exception {
        save(name, value, password);
    }

    public String read(@NonNull String name) throws Exception {
        return read(name, password);
    }

    public abstract void save(@NonNull String name,
                              @NonNull String value,
                              @NonNull String password) throws Exception;

    public abstract String read(@NonNull String name,
                                @NonNull String password) throws Exception;
}
