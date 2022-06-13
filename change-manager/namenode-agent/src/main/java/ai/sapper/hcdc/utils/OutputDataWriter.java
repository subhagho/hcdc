package ai.sapper.hcdc.utils;

import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public abstract class OutputDataWriter<T> {

    public enum EOutputFormat {
        Parquet, Avro
    }

    private final String path;
    private final String filename;
    private final FileSystem fs;
    private final EOutputFormat format;

    protected OutputDataWriter(@NonNull String path, @NonNull String filename, @NonNull FileSystem fs, @NonNull EOutputFormat format) {
        this.path = path;
        this.filename = filename;
        this.fs = fs;
        this.format = format;
    }

    public abstract void write(@NonNull Map<String, Integer> header, @NonNull List<T> records) throws IOException;
}
