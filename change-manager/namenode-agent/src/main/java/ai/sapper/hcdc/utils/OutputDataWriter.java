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
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.hadoop.fs.FileSystem;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class OutputDataWriter<T> implements Closeable {

    public enum EOutputFormat {
        Parquet, Avro;

        public static String getExt(@NonNull EOutputFormat f) {
            return f.name().toLowerCase();
        }

        public static EOutputFormat parse(@NonNull String value) {
            for (EOutputFormat f : EOutputFormat.values()) {
                if (f.name().compareToIgnoreCase(value) == 0) return f;
            }
            return null;
        }
    }

    private String path;
    private final String filename;
    private final FileSystem fs;
    private final EOutputFormat format;

    protected OutputDataWriter(@NonNull String path,
                               @NonNull String filename,
                               @NonNull FileSystem fs,
                               @NonNull EOutputFormat format) {
        this.path = path;
        this.filename = filename;
        this.fs = fs;
        this.format = format;
    }


    public static String getFilePath(@NonNull String filename) throws IOException {
        Path path = Paths.get(filename);
        BasicFileAttributes attrs = Files.readAttributes(path.toAbsolutePath(), BasicFileAttributes.class);

        FileTime ft = attrs.lastModifiedTime();
        Date dt = new Date(ft.toMillis());
        DateFormat df = new SimpleDateFormat("yyyy/MM/dd/HH/mm");

        return df.format(dt);
    }

    public static String getDatePath() throws IOException {
        Date dt = new Date();
        DateFormat df = new SimpleDateFormat("yyyy/MM/dd/HH/mm");

        return df.format(dt);
    }

    public abstract void write(String name, @NonNull Map<String, Integer> header, @NonNull List<T> records) throws IOException;

    public abstract boolean doUpload();
}
