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

package org.apache.hadoop.hdfs.tools.offlineEditsViewer;

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.DFSEditsFileFinder;
import ai.sapper.cdc.core.NameNodeEnv;
import ai.sapper.cdc.core.model.DFSAgentError;
import ai.sapper.cdc.core.model.dfs.DFSEditLogBatch;
import ai.sapper.cdc.core.model.dfs.DFSTransactionType;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.hadoop.hdfs.server.namenode.CustomEditsVisitor;

import java.util.ArrayList;
import java.util.List;

@Getter
@Accessors(fluent = true)
public class EditsLogFileReader {
    private DFSEditLogBatch batch;
    private CustomEditsVisitor visitor;

    public void run(@NonNull DFSEditsFileFinder.EditsLogFile file,
                    long startTxId,
                    long endTxId,
                    @NonNull NameNodeEnv env) throws DFSAgentError {
        try {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(file.path()));

            visitor = new CustomEditsVisitor(file.path(), env)
                    .withStartTxId(startTxId)
                    .withEndTxId(endTxId);
            OfflineEditsLoader loader = OfflineEditsLoader.OfflineEditsLoaderFactory.
                    createLoader(visitor, file.path(), false, new OfflineEditsViewer.Flags());
            loader.loadEdits();

            DFSEditLogBatch b = visitor.getBatch();
            batch = new DFSEditLogBatch(b);
            if (startTxId < 0) {
                startTxId = file.startTxId();
            }
            if (endTxId < 0) {
                endTxId = file.endTxId();
            }
            long stx = Math.max(startTxId, file.startTxId());
            long etx = Math.min(endTxId, file.endTxId());

            for (DFSTransactionType<?> tx : b.transactions()) {
                batch.transactions().add(tx);
            }
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            throw new DFSAgentError(t);
        }
    }

    public void run(@NonNull DFSEditsFileFinder.EditsLogFile file, @NonNull NameNodeEnv env) throws DFSAgentError {
        run(file, -1, -1, env);
    }

    public static List<DFSEditLogBatch> readEditsInRange(@NonNull String dir,
                                                         long startTxId,
                                                         long endTxId,
                                                         @NonNull NameNodeEnv env) throws DFSAgentError {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(dir));

        try {
            List<DFSEditLogBatch> batches = new ArrayList<>();
            List<DFSEditsFileFinder.EditsLogFile> files = DFSEditsFileFinder.findEditsFiles(dir, startTxId, endTxId);
            if (files != null && !files.isEmpty()) {
                for (DFSEditsFileFinder.EditsLogFile file : files) {
                    DefaultLogger.info(
                            String.format("Reading transactions from edits file. [%s][startTx=%d, endTx=%d]",
                                    file.path(), startTxId, endTxId));
                    EditsLogFileReader viewer = new EditsLogFileReader();
                    viewer.run(file, startTxId, endTxId, env);

                    if (viewer.batch != null) {
                        batches.add(viewer.batch);
                    }
                }
            }
            if (!batches.isEmpty()) return batches;
            return null;
        } catch (Exception ex) {
            throw new DFSAgentError(ex);
        }
    }
}
