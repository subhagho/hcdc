package ai.sapper.hcdc.utils;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.common.model.services.EConfigFileType;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.connections.ConnectionManager;
import ai.sapper.cdc.core.connections.hadoop.HdfsConnection;
import ai.sapper.cdc.core.utils.UtilsEnv;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class HadoopDataLoader {
    @Parameter(names = {"--config", "-c"}, description = "Configuration File path", required = true)
    private String configfile;
    @Parameter(names = {"--input", "-i"}, description = "Input Data Format (CSV)", required = true)
    private String inputFormat;
    @Parameter(names = {"--data", "-d"}, description = "Directory path to load data from.", required = true)
    private String dataFolder;
    @Parameter(names = {"--output", "-o"}, description = "Output Data Format (Parquet, Avro)", required = true)
    private String outputFormat;
    @Parameter(names = {"--tmp", "-t"}, description = "Temp directory to use to create local files. [DEFAULT=System.getProperty(\"java.io.tmpdir\")]")
    private String tempDir = System.getProperty("java.io.tmpdir");
    @Parameter(names = {"--batchSize", "-b"}, description = "Batch Size to read input data. [Output files will also be limited to this batch size] [DEFAULT=8192]")
    private int readBatchSize = 1024 * 16;
    private HierarchicalConfiguration<ImmutableNode> config;
    private LoaderConfig loaderConfig;
    private ConnectionManager connectionManager;
    private HdfsConnection connection;
    private FileSystem fs;
    private Path basePath;

    public void run() throws Exception {
        try {
            config = ConfigReader.read(configfile, EConfigFileType.File);
            UtilsEnv env = new UtilsEnv(getClass().getSimpleName());
            env.init(config, new UtilsEnv.UtilsState());

            loaderConfig = new LoaderConfig(env.rootConfig());
            loaderConfig.read();

            connectionManager = new ConnectionManager();
            connectionManager
                    .init(config, env, loaderConfig.connectionPath);

            connection = connectionManager.getConnection(loaderConfig.connectionToUse, HdfsConnection.class);
            connection.connect();

            fs = connection.fileSystem();

            basePath = new Path(loaderConfig.baseDir);
            if (!fs.exists(basePath)) {
                fs.mkdirs(basePath);
            }
            read(new File(dataFolder));

        } catch (Throwable t) {
            DefaultLogger.LOGGER.error(t.getLocalizedMessage());
            DefaultLogger.LOGGER.debug(DefaultLogger.stacktrace(t));
            throw new Exception(t);
        }
    }

    private void read(@NonNull File dir) throws Exception {
        Preconditions.checkArgument(dir.isDirectory());
        File[] files = dir.listFiles();
        if (files != null && files.length > 0) {
            for (File file : files) {
                if (file.isDirectory()) read(file);
                if (InputDataReader.EInputFormat.isValidFile(file.getName())) {
                    process(file);
                }
            }
        }
    }

    private void process(@NonNull File file) throws Exception {
        try (InputDataReader<List<String>> reader = getReader(file.getAbsolutePath())) {
            int index = 1;

            while (true) {
                List<List<String>> records = reader.read();
                if (records != null && !records.isEmpty()) {
                    for (List<String> record : records) {
                        record.add(UUID.randomUUID().toString());
                    }
                    Map<String, Integer> header = reader.header();
                    header.put("ADDED_COLUMN_UUID", header.size());

                    String folder = getFolderName(file);
                    String datePath = OutputDataWriter.getDatePath();
                    OutputDataWriter.EOutputFormat f = OutputDataWriter.EOutputFormat.parse(outputFormat);
                    Preconditions.checkNotNull(f);
                    String dir = String.format("%s/%s/%s", basePath, folder, datePath);
                    String tdir = String.format("%s/%s", tempDir, dir);

                    File td = new File(tdir);
                    if (!td.exists()) {
                        td.mkdirs();
                    }
                    DefaultLogger.LOGGER.debug(String.format("Writing local files to [%s]", td.getAbsolutePath()));
                    int arrayIndex = 0;
                    OutputDataWriter<List<String>> writer = null;
                    while (true) {
                        String filename = String.format("%s_%d.%s", folder, index, f.name().toLowerCase());
                        writer = getWriter(tdir, filename);
                        Preconditions.checkNotNull(writer);
                        if (!writer.doUpload()) {
                            tdir = dir;
                            writer.path(dir);
                        }
                        try {
                            List<List<String>> batch = nextBatch(records, arrayIndex);
                            if (batch == null) break;
                            writer.write(folder, header, batch);

                            if (writer.doUpload())
                                upload(fs, String.format("%s/%s", td.getAbsolutePath(), filename), dir, filename);

                            arrayIndex += batch.size();
                            index++;
                        } finally {
                            writer.close();
                        }
                    }
                } else break;
            }
        }
    }

    public static void upload(@NonNull FileSystem fs,
                              @NonNull String source,
                              @NonNull String dir,
                              @NonNull String filename) throws IOException {
        Path path = new Path(String.format("%s/%s", dir, filename));
        try (FSDataOutputStream writer = fs.create(path, true)) {
            File file = new File(source);    //creates a new file instance
            try (FileInputStream reader = new FileInputStream(file)) {  //creates a buffering character input stream
                int bsize = 8096;
                byte[] buffer = new byte[bsize];
                while (true) {
                    int r = reader.read(buffer);
                    writer.write(buffer, 0, r);
                    if (r < bsize) break;
                }
            }
        }
    }

    private List<List<String>> nextBatch(List<List<String>> records, int startIndex) {
        if (startIndex >= records.size()) return null;

        List<List<String>> batch = new ArrayList<>();
        long size = 0;
        for (int ii = startIndex; ii < records.size(); ii++) {
            long rsize = 0;
            List<String> record = records.get(ii);
            for (String r : record) {
                rsize += r.length();
            }
            if (size + rsize > loaderConfig.batchSize) break;
            batch.add(record);
            size += rsize;
        }
        return batch;
    }

    private String getFolderName(File file) {
        return FilenameUtils.removeExtension(file.getName());
    }

    private InputDataReader<List<String>> getReader(String filename) throws Exception {
        InputDataReader.EInputFormat f = InputDataReader.EInputFormat.parse(inputFormat);
        if (f == null) {
            throw new Exception(String.format("Invalid Input format type. [type=%s]", inputFormat));
        }
        switch (f) {
            case CSV:
                return new CSVDataReader(filename, ',').withBatchSize(readBatchSize);
        }
        throw new Exception(String.format("Input format not supported. [format=%s]", f.name()));
    }

    private OutputDataWriter<List<String>> getWriter(String dir, String filename) throws Exception {
        OutputDataWriter.EOutputFormat f = OutputDataWriter.EOutputFormat.parse(outputFormat);
        if (f == null) {
            throw new Exception(String.format("Invalid Output format type. [type=%s]", outputFormat));
        }
        switch (f) {
            case Parquet:
                return new ParquetDataWriter(dir, filename, fs);
            case Avro:
                return new AvroDataWriter(dir, filename, fs);
        }
        throw new Exception(String.format("Output format not supported. [format=%s]", f.name()));
    }

    @Getter
    @Accessors(fluent = true)
    public static class LoaderConfig extends ConfigReader {
        private static final String __CONFIG_PATH = "loader";
        private static final String CONFIG_CONNECTIONS = "connections.path";
        private static final String CONFIG_CONNECTION_HDFS = "connections.use";
        private static final String CONFIG_BATCH_SIZE = "batchSize";
        private static final String CONFIG_BASE_DIR = "baseDir";
        private String connectionPath;
        private long batchSize = 1024 * 1024 * 16;
        private String baseDir;
        private String connectionToUse;
        private String env;

        public LoaderConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH);
        }

        public void read() throws ConfigurationException {
            try {
                connectionPath = get().getString(CONFIG_CONNECTIONS);
                ConfigReader.checkStringValue(connectionPath, getClass(), CONFIG_CONNECTIONS);
                baseDir = get().getString(CONFIG_BASE_DIR);
                ConfigReader.checkStringValue(baseDir, getClass(), CONFIG_BASE_DIR);
                connectionToUse = get().getString(CONFIG_CONNECTION_HDFS);
                ConfigReader.checkStringValue(connectionToUse, getClass(), CONFIG_CONNECTION_HDFS);
                String s = get().getString(CONFIG_BATCH_SIZE);
                if (!Strings.isNullOrEmpty(s)) {
                    batchSize = Long.parseLong(s);
                }
            } catch (Exception ex) {
                throw new ConfigurationException(ex);
            }
        }
    }

    public static void main(String[] args) {
        try {
            HadoopDataLoader loader = new HadoopDataLoader();
            JCommander.newBuilder().addObject(loader).build().parse(args);
            loader.run();
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
}
