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
import ai.sapper.cdc.core.processing.EventProcessorMetrics;
import io.micrometer.core.instrument.Counter;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

@Getter
@Accessors(fluent = true)
public class HCdcBaseMetrics extends EventProcessorMetrics {
    public static final String PREFIX = "%s_%s";

    public static String METRICS_BACKLOG_EVENT;
    public static String METRICS_EVENT_FILE_ADD;
    public static String METRICS_EVENT_FILE_CLOSE;
    public static String METRICS_EVENT_FILE_APPEND;
    public static String METRICS_EVENT_FILE_DELETE;
    public static String METRICS_EVENT_FILE_RENAME;
    public static String METRICS_EVENT_BLOCK_ADD;
    public static String METRICS_EVENT_BLOCK_UPDATE;
    public static String METRICS_EVENT_BLOCK_DELETE;
    public static String METRICS_EVENT_BLOCK_TRUNCATE;
    public static String METRICS_EVENT_IGNORE;
    public static String METRICS_EVENT_ERROR;

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

    public HCdcBaseMetrics(@NonNull String engine,
                           @NonNull String name,
                           @NonNull String sourceType,
                           @NonNull BaseEnv<?> env,
                           @NonNull String prefix) {
        super(engine, name, sourceType, env);
        METRICS_BACKLOG_EVENT = String.format(PREFIX, prefix, "backlog");
        METRICS_EVENT_FILE_ADD = String.format(PREFIX, prefix, "file_add");
        METRICS_EVENT_FILE_CLOSE = String.format(PREFIX, prefix, "file_close");
        METRICS_EVENT_FILE_APPEND = String.format(PREFIX, prefix, "file_append");
        METRICS_EVENT_FILE_DELETE = String.format(PREFIX, prefix, "file_delete");
        METRICS_EVENT_FILE_RENAME = String.format(PREFIX, prefix, "file_rename");
        METRICS_EVENT_BLOCK_ADD = String.format(PREFIX, prefix, "block_add");
        METRICS_EVENT_BLOCK_UPDATE = String.format(PREFIX, prefix, "block_update");
        METRICS_EVENT_BLOCK_DELETE = String.format(PREFIX, prefix, "block_delete");
        METRICS_EVENT_BLOCK_TRUNCATE = String.format(PREFIX, prefix, "block_truncate");
        METRICS_EVENT_IGNORE = String.format(PREFIX, prefix, "ignore");
        METRICS_EVENT_ERROR = String.format(PREFIX, prefix, "error");

        metricsBacklog = addCounter(METRICS_BACKLOG_EVENT, null);
        metricsEventAddFile = addCounter(METRICS_EVENT_FILE_ADD, null);
        metricsEventCloseFile = addCounter(METRICS_EVENT_FILE_CLOSE, null);
        metricsEventAppendFile = addCounter(METRICS_EVENT_FILE_APPEND, null);
        metricsEventDeleteFile = addCounter(METRICS_EVENT_FILE_DELETE, null);
        metricsEventRenameFile = addCounter(METRICS_EVENT_FILE_RENAME, null);
        metricsEventAddBlock = addCounter(METRICS_EVENT_BLOCK_ADD, null);
        metricsEventUpdateBlock = addCounter(METRICS_EVENT_BLOCK_UPDATE, null);
        metricsEventDeleteBlock = addCounter(METRICS_EVENT_BLOCK_DELETE, null);
        metricsEventTruncateBlock = addCounter(METRICS_EVENT_BLOCK_TRUNCATE, null);
        metricsEventIgnore = addCounter(METRICS_EVENT_IGNORE, null);
        metricsEventError = addCounter(METRICS_EVENT_ERROR, null);
    }
}
