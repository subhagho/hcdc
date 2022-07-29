package ai.sapper.hcdc.agents.common.converter;

import ai.sapper.hcdc.agents.common.FormatConverter;
import lombok.NonNull;
import org.apache.commons.io.FilenameUtils;
import org.apache.parquet.Strings;

import java.io.File;
import java.io.IOException;

public class AvroConverter implements FormatConverter {
    public static final String EXT = "avro";

    /**
     * @param path
     * @return
     * @throws IOException
     */
    @Override
    public boolean canParse(@NonNull String path) throws IOException {
        String ext = FilenameUtils.getExtension(path);
        return (!Strings.isNullOrEmpty(ext) && ext.compareToIgnoreCase(EXT) == 0);
    }

    /**
     * @param source
     * @param output
     * @throws IOException
     */
    @Override
    public File convert(@NonNull File source, @NonNull File output) throws IOException {
        return source;
    }

    /**
     * @return
     */
    @Override
    public boolean supportsPartial() {
        return true;
    }
}
