package ai.sapper.hcdc.agents.common;

import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.StateManagerError;
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
     * @param env
     * @return
     * @throws StateManagerError
     */
    @Override
    public ZkStateManager init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                               @NonNull BaseEnv<?> env,
                               @NonNull String source) throws StateManagerError {
        super.init(xmlConfig, env, source);
        try {
            domainManager = new DomainManager();
            domainManager.init(xmlConfig, env.connectionManager(), environment());

            return this;
        } catch (Exception ex) {
            throw new StateManagerError(ex);
        }
    }
}
