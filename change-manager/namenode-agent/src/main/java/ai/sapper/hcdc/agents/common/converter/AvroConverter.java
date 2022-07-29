package ai.sapper.hcdc.agents.common.converter;

import ai.sapper.hcdc.agents.common.FormatConverter;
import ai.sapper.hcdc.core.model.EFileType;
import lombok.NonNull;
import org.apache.commons.io.FilenameUtils;
import org.apache.parquet.Strings;

import java.io.File;
import java.io.IOException;

public class AvroConverter implements FormatConverter {
    public static final String EXT = "avro";

    /**
     * @param path
     * @param fileType
     * @return
     * @throws IOException
     */
    @Override
    public boolean canParse(@NonNull String path, EFileType fileType) throws IOException {
        if (fileType == EFileType.AVRO) return true;
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

    /**
     * @param data
     * @param length
     * @return
     * @throws IOException
     */
    @Override
    public boolean detect(byte[] data, int length) throws IOException {
        return false;
    }

    /**
     * @return
     */
    @Override
    public EFileType fileType() {
        return EFileType.AVRO;
    }
}
