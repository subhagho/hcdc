package ai.sapper.hcdc.utils;

import ai.sapper.cdc.common.utils.DefaultLogger;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReaderHeaderAware;
import com.opencsv.CSVReaderHeaderAwareBuilder;
import lombok.NonNull;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CSVDataReader extends InputDataReader<List<String>> {

    private char separator = ',';
    private CSVReaderHeaderAware reader = null;

    public CSVDataReader(@NonNull String filename, char separator) {
        super(filename, EInputFormat.CSV);
        if (separator != Character.MIN_VALUE)
            this.separator = separator;
    }

    /**
     * @throws IOException
     */
    @Override
    public List<List<String>> read() throws IOException {
        try {
            if (reader == null) {
                CSVParser parser = new CSVParserBuilder().withSeparator(separator).build();
                reader
                        = new CSVReaderHeaderAwareBuilder(new FileReader(filename()))
                        .withCSVParser(parser)
                        .build();
            }
            return read(reader);

        } catch (Throwable t) {
            DefaultLogger.LOGGER.debug(DefaultLogger.stacktrace(t));
            throw new IOException(t);
        }
    }

    private List<List<String>> read(CSVReaderHeaderAware reader) throws Exception {
        Map<String, String> values = null;
        List<List<String>> records = new ArrayList<>();

        int readCount = 0;
        while (true) {
            try {
                values = reader.readMap();
                if (values == null) break;
                readCount++;
                if (batchSize() > 0 && readCount >= batchSize()) break;
            } catch (IOException ex) {
                DefaultLogger.LOGGER.error(ex.getLocalizedMessage());
                readCount++;
                continue;
            }

            List<String> array = new ArrayList<>();
            int nextIndex = 0;
            if (header().isEmpty()) {
                for (String key : values.keySet()) {
                    if (!header().containsKey(key)) {
                        header().put(key, nextIndex);
                        nextIndex++;
                    }
                }
            }
            for (String key : values.keySet()) {
                int indx = header().get(key);
                array.add(indx, values.get(key));
            }
            records.add(array);
        }
        startIndex += readCount;

        if (!records.isEmpty()) return records;
        return null;
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
        if (reader != null) {
            reader.close();
            reader = null;
        }
    }
}
