package ai.sapper.hcdc.agents.common;

import ai.sapper.cdc.core.connections.ConnectionManager;
import ai.sapper.cdc.core.filters.DomainManager;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

@Getter
@Accessors(fluent = true)
public class ProcessorStateManager extends ZkStateManager {
    private DomainManager domainManager;

    /**
     * @param xmlConfig
     * @param manger
     * @return
     * @throws StateManagerError
     */
    @Override
    public ZkStateManager init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                               @NonNull ConnectionManager manger,
                               @NonNull String source) throws StateManagerError {
        super.init(xmlConfig, manger, source);
        try {
            domainManager = new DomainManager();
            domainManager.init(xmlConfig, manger, environment());

            return this;
        } catch (Exception ex) {
            throw new StateManagerError(ex);
        }
    }
}
