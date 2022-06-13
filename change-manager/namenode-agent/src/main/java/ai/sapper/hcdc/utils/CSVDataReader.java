package ai.sapper.hcdc.utils;

import ai.sapper.hcdc.common.utils.DefaultLogger;
import com.google.common.base.Strings;
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

    public CSVDataReader(@NonNull String filename, char separator) {
        super(filename, EDataType.CVS);
        this.separator = separator;
    }

    /**
     * @throws IOException
     */
    @Override
    public void read() throws IOException {
        try {
            CSVParser parser = new CSVParserBuilder().withSeparator(separator).build();
            try (CSVReaderHeaderAware reader = new CSVReaderHeaderAwareBuilder(new FileReader(filename())).withCSVParser(parser).build()) {
                read(reader);
            }
        } catch (Throwable t) {
            DefaultLogger.__LOG.debug(DefaultLogger.stacktrace(t));
            throw new IOException(t);
        }
    }

    private void read(CSVReaderHeaderAware reader) throws Exception {
        Map<String, String> values;
        while ((values = reader.readMap()) != null) {
            List<String> array = new ArrayList<>();
            int nextIndex = 0;
            for (String key : values.keySet()) {
                if (!header().containsKey(key)) {
                    header().put(key, nextIndex);
                    nextIndex++;
                }
                int indx = header().get(key);
                array.add(indx, values.get(key));
            }
            records().add(array);
        }
    }
}
