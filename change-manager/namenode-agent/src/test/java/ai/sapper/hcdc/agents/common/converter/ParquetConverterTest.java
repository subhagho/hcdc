package ai.sapper.hcdc.agents.common.converter;

import ai.sapper.hcdc.common.utils.DefaultLogger;
import org.apache.commons.io.FilenameUtils;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

class ParquetConverterTest {
    private static final String FILE = "src/test/resources/data/userdata2.parquet";
    private static final String OUTDIR = "C:\\Work\\temp\\output";

    @Test
    void convert() {
        try {
            ParquetConverter converter = new ParquetConverter();
            File inf = new File(FILE);
            String name = FilenameUtils.getName(inf.getAbsolutePath());
            name = FilenameUtils.removeExtension(name);
            File outf = new File(String.format("%s/%s.avro", OUTDIR, name));
            converter.convert(inf, outf);

            DefaultLogger.LOG.info(String.format("Generated output: [path=%s]", outf.getAbsolutePath()));
        } catch (Throwable t) {
            DefaultLogger.LOG.debug(DefaultLogger.stacktrace(t));
            fail(t);
        }
    }
}