package ai.sapper.hcdc.utils;

import ai.sapper.hcdc.common.utils.DefaultLogger;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.Strings;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
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
    public void write(String name, @NonNull Map<String, Integer> header, @NonNull List<List<String>> records) throws IOException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        String file = String.format("%s/%s", path(), filename());
        File f = new File(file);
        if (f.exists()) {
            f.delete();
        }
        try {
            MessageType mt = getSchema(name, header);
            Path path = new Path(path());
            if (!fs().exists(path)) {
                fs().mkdirs(path);
            }
            path = new Path(file);

            try (ParquetWriter<List<String>> writer = new ParquetWriter<>(path, new CustomWriteSupport(mt),
                    CompressionCodecName.SNAPPY, ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE,
                    true, true)) {
                for (List<String> record : records) {
                    writer.write(record);
                }
            }

        } catch (Exception ex) {
            DefaultLogger.LOG.debug(DefaultLogger.stacktrace(ex));
            throw new IOException(ex);
        }
    }

    private MessageType getSchema(String name, Map<String, Integer> header) throws Exception {
        List<Type> types = new ArrayList<>(header.size());
        for (String key : header.keySet()) {
            Type t = new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, key);
            types.add(t);
        }
        return new MessageType(name, types);
    }

    /**
     * Closes this stream and releases any system resources associated
     * with it. If the stream is already closed then invoking this
     * method has no effect.
     *
     * <p> As noted in {@link AutoCloseable#close()}, cases where the
     * close may fail require careful attention. It is strongly advised
     * to relinquish the underlying resources and to internally
     * <em>mark</em> the {@code Closeable} as closed, prior to throwing
     * the {@code IOException}.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {

    }
}
