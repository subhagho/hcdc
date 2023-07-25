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

package ai.sapper.cdc.core.model;

import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.NameNodeEnv;
import ai.sapper.cdc.core.processing.EventProcessorMetrics;
import io.micrometer.core.instrument.Counter;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.util.Map;

@Getter
@Accessors(fluent = true)
public class HCdcBaseMetrics extends EventProcessorMetrics {
    public static final String METRIC_TAG_NAME = "process_name";

    public static final String METRICS_BACKLOG_EVENT = "hdfs_backlog";
    public static final String METRICS_EVENT_FILE_ADD = "hdfs_file_add";
    public static final String METRICS_EVENT_FILE_CLOSE = "hdfs_file_close";
    public static final String METRICS_EVENT_FILE_APPEND = "hdfs_file_append";
    public static final String METRICS_EVENT_FILE_DELETE = "hdfs_file_delete";
    public static final String METRICS_EVENT_FILE_RENAME = "hdfs_file_rename";
    public static final String METRICS_EVENT_BLOCK_ADD = "hdfs_block_add";
    public static final String METRICS_EVENT_BLOCK_UPDATE = "hdfs_block_update";
    public static final String METRICS_EVENT_BLOCK_DELETE = "hdfs_block_delete";
    public static final String METRICS_EVENT_BLOCK_TRUNCATE = "hdfs_block_truncate";
    public static final String METRICS_EVENT_IGNORE = "hdfs_ignore";
    public static final String METRICS_EVENT_ERROR = "hdfs_error";

    private final Counter metricsBacklog;
    private final Counter metricsEventAddFile;
    private final Counter metricsEventCloseFile;
    private final Counter metricsEventAppendFile;
    private final Counter metricsEventDeleteFile;
    private final Counter metricsEventRenameFile;
    private final Counter metricsEventAddBlock;
    private final Counter metricsEventUpdateBlock;
    private final Counter metricsEventDeleteBlock;
    private final Counter metricsEventTruncateBlock;
    private final Counter metricsEventIgnore;
    private final Counter metricsEventError;

    public HCdcBaseMetrics(@NonNull String name,
                           @NonNull BaseEnv<?> env,
                           @NonNull String process) {
        super(NameNodeEnv.Constants.ENGINE_TYPE,
                name,
                NameNodeEnv.Constants.DB_TYPE,
                env);

        metricsBacklog = addCounter(METRICS_BACKLOG_EVENT, Map.of(METRIC_TAG_NAME, process));
        metricsEventAddFile = addCounter(METRICS_EVENT_FILE_ADD, Map.of(METRIC_TAG_NAME, process));
        metricsEventCloseFile = addCounter(METRICS_EVENT_FILE_CLOSE, Map.of(METRIC_TAG_NAME, process));
        metricsEventAppendFile = addCounter(METRICS_EVENT_FILE_APPEND, Map.of(METRIC_TAG_NAME, process));
        metricsEventDeleteFile = addCounter(METRICS_EVENT_FILE_DELETE, Map.of(METRIC_TAG_NAME, process));
        metricsEventRenameFile = addCounter(METRICS_EVENT_FILE_RENAME, Map.of(METRIC_TAG_NAME, process));
        metricsEventAddBlock = addCounter(METRICS_EVENT_BLOCK_ADD, Map.of(METRIC_TAG_NAME, process));
        metricsEventUpdateBlock = addCounter(METRICS_EVENT_BLOCK_UPDATE, Map.of(METRIC_TAG_NAME, process));
        metricsEventDeleteBlock = addCounter(METRICS_EVENT_BLOCK_DELETE, Map.of(METRIC_TAG_NAME, process));
        metricsEventTruncateBlock = addCounter(METRICS_EVENT_BLOCK_TRUNCATE, Map.of(METRIC_TAG_NAME, process));
        metricsEventIgnore = addCounter(METRICS_EVENT_IGNORE, Map.of(METRIC_TAG_NAME, process));
        metricsEventError = addCounter(METRICS_EVENT_ERROR, Map.of(METRIC_TAG_NAME, process));
    }
}
