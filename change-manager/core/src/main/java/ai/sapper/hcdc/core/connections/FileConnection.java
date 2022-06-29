package ai.sapper.hcdc.core.connections;

import ai.sapper.hcdc.common.ConfigReader;
import ai.sapper.hcdc.common.utils.DefaultLogger;
import ai.sapper.hcdc.common.utils.UnitsParser;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class FileConnection implements Connection {
    private FileConnConfig config;
    private final ConnectionState state = new ConnectionState();
    private final List<File> files = new ArrayList<>();
    private int readFileIndex = 0;
    private FileOutputStream writer = null;
    private long bytesWritten = 0;

    /**
     * @return
     */
    @Override
    public String name() {
        return config.name();
    }

    /**
     * @param xmlConfig
     * @return
     * @throws ConnectionError
     */
    @Override
    public Connection init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConnectionError {
        try {
            config = new FileConnConfig(xmlConfig);
            config.read();

            state.state(EConnectionState.Initialized);
            return this;
        } catch (Exception ex) {
            state.error(ex);
            throw new ConnectionError(ex);
        }
    }

    /**
     * @return
     * @throws ConnectionError
     */
    @Override
    public Connection connect() throws ConnectionError {
        checkFilesToRead();
        state.state(EConnectionState.Connected);
        return this;
    }

    private void checkFilesToRead() {
        files.clear();
        File[] fs = config.dir.listFiles();
        if (fs != null && fs.length > 0) {
            for (File f : fs) {
                if (f.isFile()) {
                    String name = f.getName();
                    if (name.startsWith(config.prefix)) {
                        files.add(f);
                    }
                }
            }
        }
    }

    /**
     * @return
     */
    @Override
    public Throwable error() {
        return state.error();
    }

    /**
     * @return
     */
    @Override
    public EConnectionState connectionState() {
        return state.state();
    }

    /**
     * @return
     */
    @Override
    public boolean isConnected() {
        return (state.isConnected());
    }

    /**
     * @return
     */
    @Override
    public HierarchicalConfiguration<ImmutableNode> config() {
        return config.config();
    }

    public File next() {
        Preconditions.checkState(state.isConnected());
        File f = null;
        if (readFileIndex > files.size()) {
            readFileIndex = 0;
            checkFilesToRead();
        }
        if (readFileIndex < files.size()) {
            f = files.get(readFileIndex);
            readFileIndex++;
        }
        return f;
    }

    public void write(byte[] data) throws IOException {
        Preconditions.checkArgument(data != null && data.length > 0);
        Preconditions.checkState(state.isConnected());
        Preconditions.checkState(!config.readOnly);

        checkFileState();
        writer.write(data);

        bytesWritten += data.length;
    }

    private void checkFileState() throws IOException {
        if (config.maxFileSize > 0 && bytesWritten >= config.maxFileSize()) {
            open();
        }
    }

    private String filename() {
        StringBuilder sb = new StringBuilder(String.format("%s_%s", config.prefix, UUID.randomUUID().toString()));
        if (!Strings.isNullOrEmpty(config.ext)) {
            sb.append('.').append(config.ext);
        }
        return sb.toString();
    }

    private void open() throws IOException {
        bytesWritten = 0;
        if (writer != null) {
            writer.close();
        }
        String fname = filename();
        String file = String.format("%s/%s", config.dir.getAbsolutePath(), fname);
        DefaultLogger.LOG.info(String.format("Opened new file for write. [path=%s]", file));

        writer = new FileOutputStream(file);
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
        if (writer != null) {
            writer.close();
            writer = null;
        }
        files.clear();
        if (!state.hasError()) {
            state.state(EConnectionState.Closed);
        }
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class FileConnConfig extends ConfigReader {
        private static final String __CONFIG_PATH = "file";
        private static final String CONFIG_NAME = "name";
        private static final String CONFIG_PATH = "path";
        private static final String CONFIG_FILE_PREFIX = "prefix";
        private static final String CONFIG_FILE_EXT = "extension";
        private static final String CONFIG_MAX_FILE_SIZE = "maxSize";
        private static final String CONFIG_BACKUP = "backup";
        private static final String CONFIG_READ_ONLY = "readOnly";
        private String name;
        private File dir;
        private String prefix;
        private String ext;
        private UnitsParser.UnitValue maxSize;
        private boolean backup = false;
        private boolean readOnly = false;
        private long maxFileSize = -1;

        public FileConnConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH);
        }

        public void read() throws ConfigurationException {
            name = config().getString(CONFIG_NAME);
            if (Strings.isNullOrEmpty(name)) {
                throw new ConfigurationException(String.format("File Configuration Error : Missing configuration [name=%s]", CONFIG_NAME));
            }
            String dn = config().getString(CONFIG_PATH);
            if (Strings.isNullOrEmpty(dn)) {
                throw new ConfigurationException(String.format("File Configuration Error : Missing configuration [name=%s]", CONFIG_PATH));
            }
            dir = new File(dn);
            if (!dir.exists()) {
                throw new ConfigurationException(String.format("Specified path does not exist. [path=%s]", dir.getAbsolutePath()));
            }

            prefix = config().getString(CONFIG_FILE_PREFIX);
            if (Strings.isNullOrEmpty(prefix)) {
                throw new ConfigurationException(String.format("File Configuration Error : Missing configuration [name=%s]", CONFIG_FILE_PREFIX));
            }
            ext = config().getString(CONFIG_FILE_EXT);
            String s = config().getString(CONFIG_MAX_FILE_SIZE);
            if (!Strings.isNullOrEmpty(s)) {
                maxSize = UnitsParser.parse(s);
                if (maxSize == null) {
                    throw new ConfigurationException(String.format("Invalid Max Size : [value=%s]", s));
                }
                maxFileSize = UnitsParser.dataSize(maxSize);
            }
            s = config().getString(CONFIG_BACKUP);
            if (!Strings.isNullOrEmpty(s)) {
                backup = Boolean.parseBoolean(s);
            }
            s = config().getString(CONFIG_READ_ONLY);
            if (!Strings.isNullOrEmpty(s)) {
                readOnly = Boolean.parseBoolean(s);
            }
        }
    }
}
