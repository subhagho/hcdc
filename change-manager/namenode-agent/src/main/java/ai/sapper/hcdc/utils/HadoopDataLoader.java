package ai.sapper.hcdc.utils;

import ai.sapper.hcdc.common.ConfigReader;
import ai.sapper.hcdc.common.utils.DefaultLogger;
import ai.sapper.hcdc.core.connections.ConnectionManager;
import ai.sapper.hcdc.core.connections.HdfsConnection;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.naming.ConfigurationException;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class HadoopDataLoader {
    @Parameter(names = {"--config", "-c"}, description = "Configuration File path", required = true)
    private String configfile;
    @Parameter(names = {"--input", "-i"}, description = "Input Data Format (CSV)", required = true)
    private String inputFormat;
    @Parameter(names = {"--data", "-d"}, description = "Directory path to load data from.", required = true)
    private String dataFolder;
    @Parameter(names = {"--output", "-o"}, description = "Output Data Format (Parquet, Avro)", required = true)
    private String outputFormat;
    private HierarchicalConfiguration<ImmutableNode> config;
    private LoaderConfig loaderConfig;
    private ConnectionManager connectionManager;
    private HdfsConnection connection;
    private FileSystem fs;
    private Path basePath;

    public void run() throws Exception {
        try {
            config = readConfigFile(configfile);
            loaderConfig = new LoaderConfig(config);
            loaderConfig.read();

            connectionManager = new ConnectionManager();
            connectionManager.init(config, loaderConfig.connectionPath);

            connection = connectionManager.getConnection(loaderConfig.connectionToUse, HdfsConnection.class);
            connection.connect();

            fs = connection.fileSystem();
            basePath = new Path(loaderConfig.baseDir);
            if (!fs.exists(basePath)) {
                fs.mkdirs(basePath);
            }
            read(new File(dataFolder));

        } catch (Throwable t) {
            DefaultLogger.__LOG.error(t.getLocalizedMessage());
            DefaultLogger.__LOG.debug(DefaultLogger.stacktrace(t));
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
        InputDataReader<List<String>> reader = getReader(file.getAbsolutePath());
        reader.read();

        List<List<String>> records = reader.records();
        if (records != null && !records.isEmpty()) {
            String folder = getFolderName(file);
            String datePath = OutputDataWriter.getFilePath(file.getAbsolutePath());
            OutputDataWriter.EOutputFormat f = OutputDataWriter.EOutputFormat.parse(outputFormat);
            Preconditions.checkNotNull(f);
            String dir = String.format("%s/%s/%s", basePath, folder, datePath);
            int index = 1;
            int arrayIndex = 0;
            OutputDataWriter<List<String>> writer = null;
            while (true) {
                String filename = String.format("%s_%d.%s", folder, index, f.name().toLowerCase());
                writer = getWriter(dir, filename);

                List<List<String>> batch = nextBatch(records, arrayIndex);
                if (batch == null) break;
                writer.write(folder, reader.header(), batch);

                arrayIndex += batch.size();
                index++;
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
                rsize += (r.length() * 8L);
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

    private XMLConfiguration readConfigFile(@NonNull String configFile) throws Exception {
        File cf = new File(configFile);
        if (!cf.exists()) {
            throw new Exception(String.format("Configuration file not found. ]path=%s]", cf.getAbsolutePath()));
        }
        Configurations configs = new Configurations();
        return configs.xml(cf);
    }

    private InputDataReader<List<String>> getReader(String filename) throws Exception {
        InputDataReader.EInputFormat f = InputDataReader.EInputFormat.parse(inputFormat);
        if (f == null) {
            throw new Exception(String.format("Invalid Input format type. [type=%s]", inputFormat));
        }
        switch (f) {
            case CSV:
                return new CSVDataReader(filename, ',');
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

        public LoaderConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH);
        }

        public void read() throws ConfigurationException {
            connectionPath = get().getString(CONFIG_CONNECTIONS);
            if (Strings.isNullOrEmpty(connectionPath)) {
                throw new ConfigurationException(String.format("HDFS Data Loader Configuration Error: missing [%s]", CONFIG_CONNECTIONS));
            }
            baseDir = get().getString(CONFIG_BASE_DIR);
            if (Strings.isNullOrEmpty(baseDir)) {
                throw new ConfigurationException(String.format("HDFS Data Loader Configuration Error: missing [%s]", CONFIG_BASE_DIR));
            }
            connectionToUse = get().getString(CONFIG_CONNECTION_HDFS);
            if (Strings.isNullOrEmpty(connectionToUse)) {
                throw new ConfigurationException(String.format("HDFS Data Loader Configuration Error: missing [%s]", CONFIG_CONNECTION_HDFS));
            }
            String s = get().getString(CONFIG_BATCH_SIZE);
            if (!Strings.isNullOrEmpty(s)) {
                batchSize = Long.parseLong(s);
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
