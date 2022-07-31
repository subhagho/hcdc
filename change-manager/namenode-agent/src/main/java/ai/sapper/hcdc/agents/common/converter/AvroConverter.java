package ai.sapper.hcdc.agents.common.converter;

import ai.sapper.hcdc.agents.common.FormatConverter;
import ai.sapper.hcdc.common.utils.PathUtils;
import ai.sapper.hcdc.core.model.DFSBlockState;
import ai.sapper.hcdc.core.model.DFSFileState;
import ai.sapper.hcdc.core.model.EFileType;
import ai.sapper.hcdc.core.model.HDFSBlockData;
import lombok.NonNull;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.hdfs.HDFSBlockReader;
import org.apache.parquet.Strings;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class AvroConverter implements FormatConverter {
    private static final String MAGIC_CODE = "Obj1";
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
        if (data != null && length >= 4) {
            String m = new String(data, 0, 4, StandardCharsets.UTF_8);
            return m.compareTo(MAGIC_CODE) == 0;
        }
        return false;
    }

    /**
     * @return
     */
    @Override
    public EFileType fileType() {
        return EFileType.AVRO;
    }

    /**
     * @param reader
     * @param outdir
     * @return
     * @throws IOException
     */
    @Override
    public File extractSchema(@NonNull HDFSBlockReader reader,
                              @NonNull File outdir,
                              @NonNull DFSFileState fileState) throws IOException {
        try {
            DFSBlockState firstBlock = fileState.findFirstBlock();
            if (firstBlock != null) {
                HDFSBlockData data = reader.read(firstBlock.getBlockId(),
                        firstBlock.getGenerationStamp(),
                        0L,
                        -1);

                if (data == null) {
                    throw new IOException(String.format("Error reading block from HDFS. [path=%s][block ID=%d]",
                            fileState.getHdfsFilePath(), firstBlock.getBlockId()));
                }
                File tempf = PathUtils.getTempFileWithExt("parquet");
                try (FileOutputStream fos = new FileOutputStream(tempf)) {
                    fos.write(data.data().array());
                    fos.flush();
                }
                DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
                try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(tempf, datumReader)) {
                    Schema schema = dataFileReader.getSchema();
                    String json = schema.toString(true);
                    File file = new File(String.format("%s/%d.avsc", outdir.getAbsolutePath(), fileState.getId()));
                    try (FileOutputStream fos = new FileOutputStream(file)) {
                        fos.write(json.getBytes(StandardCharsets.UTF_8));
                        fos.flush();
                    }
                    return file;
                }
            }
            return null;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }
}
