/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.sapper.cdc.core.state;

import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.*;
import ai.sapper.cdc.core.model.EHCdcProcessorState;
import ai.sapper.cdc.core.model.HCdcProcessingState;
import ai.sapper.cdc.core.model.HCdcTxId;
import ai.sapper.cdc.core.model.Heartbeat;
import ai.sapper.cdc.core.processing.ProcessStateManager;
import ai.sapper.cdc.core.processing.ProcessingState;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;

@Getter
@Accessors(fluent = true)
public class HCdcStateManager extends ProcessStateManager<EHCdcProcessorState, HCdcTxId> {

    public HCdcStateManager() {
        super(HCdcProcessingState.class);
    }

    protected HCdcStateManager(@NonNull Class<? extends ProcessingState<EHCdcProcessorState, HCdcTxId>> stateType) {
        super(stateType);
    }

    public static class Constants {
        public static final String ZK_PATH_FILES = "/files";
        public static final String ZK_PATH_REPLICATION = "/replication";
    }

    private String source;

    private final ReplicationStateHelper replicaStateHelper = new ReplicationStateHelper();
    private final FileStateHelper fileStateHelper = new FileStateHelper();


    public HCdcStateManager init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                 @NonNull BaseEnv<?> env) throws StateManagerError {
        Preconditions.checkState(!Strings.isNullOrEmpty(name()));
        try {
            super.init(xmlConfig,
                    BaseStateManagerSettings.__CONFIG_PATH,
                    env,
                    HCdcStateManagerSettings.class);
            NameNodeEnvSettings envSettings = (NameNodeEnvSettings) env.settings();
            this.source = envSettings.getSource();
            CuratorFramework client = connection().client();
            String zkFSPath = new PathUtils.ZkPathBuilder(zkModulePath())
                    .withPath(Constants.ZK_PATH_FILES)
                    .withPath(source)
                    .build();
            if (client.checkExists().forPath(zkFSPath) == null) {
                String path = client.create().creatingParentContainersIfNeeded().forPath(zkFSPath);
                if (Strings.isNullOrEmpty(path)) {
                    throw new StateManagerError(String.format("Error creating ZK base path. [path=%s]", basePath()));
                }
            }
            fileStateHelper
                    .withZkPath(zkFSPath)
                    .withZkConnection(connection());
            String zkPathReplication = new PathUtils.ZkPathBuilder(zkModulePath())
                    .withPath(Constants.ZK_PATH_REPLICATION)
                    .withPath(source)
                    .build();
            if (client.checkExists().forPath(zkPathReplication) == null) {
                String path = client.create().creatingParentContainersIfNeeded().forPath(zkPathReplication);
                if (Strings.isNullOrEmpty(path)) {
                    throw new StateManagerError(String.format("Error creating ZK replication path. [path=%s]", basePath()));
                }
            }
            replicaStateHelper
                    .withZkConnection(connection())
                    .withZkPath(zkPathReplication);
            processingState().setState(EHCdcProcessorState.Running);
            update(processingState());
            return this;
        } catch (Exception ex) {
            throw new StateManagerError(ex);
        }
    }

    public void deleteAll() throws StateManagerError {
        checkState();
        try {
            stateLock();
            try {
                fileStateHelper.deleteAll();
                replicaStateHelper.deleteAll();
            } finally {
                stateUnlock();
            }
        } catch (Exception ex) {
            throw new StateManagerError(ex);
        }
    }


    public String basePath() {
        return settings().getBasePath();
    }

    @Override
    public Heartbeat heartbeat(@NonNull String instance) throws StateManagerError {
        try {
            return heartbeat(instance, NameNodeEnv.get(name()).agentState());
        } catch (Exception ex) {
            throw new StateManagerError(ex);
        }
    }
}
