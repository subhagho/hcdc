package ai.sapper.cdc.core.connections;

import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFrameworkFactory;

public interface ZookeeperAuthHandler {
    void setup(@NonNull CuratorFrameworkFactory.Builder builder,
               @NonNull HierarchicalConfiguration<ImmutableNode> config) throws ConnectionError;
}
