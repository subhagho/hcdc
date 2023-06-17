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

package ai.sapper.hcdc.utils;

import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public abstract class InputDataReader<T> implements Closeable {
    public enum EInputFormat {
        CSV;

        public static boolean isValidFile(@NonNull String filename) {
            String[] parts = filename.split("\\.");
            if (parts.length > 1) {
                String ext = parts[parts.length - 1];
                for (EInputFormat f : EInputFormat.values()) {
                    if (f.name().compareToIgnoreCase(ext) == 0) return true;
                }
            }
            return false;
        }

        public static EInputFormat parse(@NonNull String value) {
            for (EInputFormat f : EInputFormat.values()) {
                if (f.name().compareToIgnoreCase(value) == 0) return f;
            }
            return null;
        }
    }

    private final EInputFormat dataType;
    private final String filename;
    private final Map<String, Integer> header = new HashMap<>();
    protected int startIndex = 0;
    private long batchSize = -1;

    public InputDataReader(@NonNull String filename, @NonNull InputDataReader.EInputFormat dataType) {
        this.filename = filename;
        this.dataType = dataType;
    }

    public InputDataReader<T> withBatchSize(long batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public abstract List<T> read() throws IOException;
}
