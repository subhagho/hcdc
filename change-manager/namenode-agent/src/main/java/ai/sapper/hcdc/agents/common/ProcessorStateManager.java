package ai.sapper.hcdc.agents.common;

import ai.sapper.hcdc.core.connections.ConnectionManager;
import ai.sapper.hcdc.core.filters.DomainManager;
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
     * @param module
     * @return
     * @throws StateManagerError
     */
    @Override
    public ZkStateManager init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                               @NonNull ConnectionManager manger,
                               @NonNull String module,
                               @NonNull String instance) throws StateManagerError {
        super.init(xmlConfig, manger, module, instance);
        try {
            domainManager = new DomainManager();
            domainManager.init(xmlConfig, manger);

            return this;
        } catch (Exception ex) {
            throw new StateManagerError(ex);
        }
    }
}
