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
import ai.sapper.cdc.core.model.*;
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
        public static final String OFFSET_SHARED_SNAPSHOT = "snapshot";
    }

    private String source;

    private ReplicationStateHelper replicaStateHelper;
    private final FileStateHelper fileStateHelper = new FileStateHelper();
    private NameNodeEnv env;


    public HCdcStateManager init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                 @NonNull BaseEnv<?> env) throws StateManagerError {
        Preconditions.checkState(!Strings.isNullOrEmpty(name()));
        try {
            super.init(xmlConfig,
                    BaseStateManagerSettings.__CONFIG_PATH,
                    env,
                    HCdcStateManagerSettings.class);
            this.env = (NameNodeEnv) env;
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
            SnapshotOffset so = checkAndCreateOffset(Constants.OFFSET_SHARED_SNAPSHOT, SnapshotOffset.class);
            fileStateHelper
                    .withZkPath(zkFSPath)
                    .withZkConnection(connection());


            processingState().setState(EHCdcProcessorState.Running);
            update(processingState());
            return this;
        } catch (Exception ex) {
            if (processingState() != null)
                processingState().error(ex);
            throw new StateManagerError(ex);
        }
    }

    public void postInit() throws Exception {
        HCdcStateManagerSettings settings = (HCdcStateManagerSettings) settings();
        if (!settings.isRequireFileState()) {
            return;
        }
        if (env.schemaManager() == null) {
            throw new Exception(String.format("[env=%s] Schema manager not configured...", env.name()));
        }
        CuratorFramework client = connection().client();
        if (this.env.schemaManager() != null) {
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
            replicaStateHelper = new ReplicationStateHelper(this.env, fileStateHelper)
                    .withZkConnection(connection())
                    .withZkPath(zkPathReplication);
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

    public EHCdcProcessorState parseState(@NonNull String value) {
        EHCdcProcessorState s = null;
        for (EHCdcProcessorState e : EHCdcProcessorState.values()) {
            if (e.name().compareToIgnoreCase(value) == 0) {
                s = e;
                break;
            }
        }
        if (s == null) {
            s = processingState().getInitState();
        }
        processingState().setState(s);
        return processingState().getState();
    }

    public SnapshotOffset getSnapshotOffset() throws StateManagerError {
        return readOffset(Constants.OFFSET_SHARED_SNAPSHOT, SnapshotOffset.class);
    }

    public SnapshotOffset updateSnapshotOffset(@NonNull SnapshotOffset offset) throws StateManagerError {
        return updateOffset(Constants.OFFSET_SHARED_SNAPSHOT, offset);
    }

    public String basePath() {
        return settings().getBasePath();
    }

    @Override
    public Heartbeat heartbeat(@NonNull String instance) throws StateManagerError {
        try {
            return heartbeat(instance, getClass(), processingState());
        } catch (Exception ex) {
            throw new StateManagerError(ex);
        }
    }
}