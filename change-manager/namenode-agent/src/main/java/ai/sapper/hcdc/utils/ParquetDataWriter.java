package ai.sapper.hcdc.utils;

import lombok.NonNull;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ParquetDataWriter extends OutputDataWriter<List<String>> {

    protected ParquetDataWriter(@NonNull String path, @NonNull String filename, @NonNull FileSystem fs) {
        super(path, filename, fs, EOutputFormat.Parquet);
    }

    /**
     * @param header
     * @param records
     * @throws IOException
     */
    @Override
    public void write(@NonNull Map<String, Integer> header, @NonNull List<List<String>> records) throws IOException {

    }
}
