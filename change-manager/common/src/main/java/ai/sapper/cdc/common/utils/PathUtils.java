package ai.sapper.cdc.common.utils;

import com.google.common.base.Strings;
import lombok.NonNull;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class PathUtils {
    public static final String TEMP_PATH = String.format("%s/hcdc/tmp", System.getProperty("java.io.tmpdir"));


    public static String formatPath(@NonNull String path) {
        path = path.replaceAll("\\\\", "/");
        path = path.replaceAll("/\\s*/", "/");
        return path;
    }

    public static File getTempDir() {
        File dir = new File(TEMP_PATH);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        return dir;
    }

    public static File getTempFile(@NonNull String name, @NonNull String ext) {
        File dir = new File(TEMP_PATH);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        File path = new File(String.format("%s/%s.%s",
                dir.getAbsolutePath(), name, ext));
        if (path.exists()) {
            path.delete();
        }
        return path;
    }

    public static File getTempFile(@NonNull String name) {
        File dir = new File(TEMP_PATH);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        File path = new File(String.format("%s/%s",
                dir.getAbsolutePath(), name));
        if (path.exists()) {
            path.delete();
        }
        return path;
    }

    public static File getTempFile() {
        return getTempFile(UUID.randomUUID().toString(), "tmp");
    }

    public static File getTempFileWithName(@NonNull String name) {
        File dir = new File(TEMP_PATH);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        File path = new File(String.format("%s/%s",
                dir.getAbsolutePath(), name));
        if (path.exists()) {
            path.delete();
        }
        return path;
    }

    public static File getTempFileWithExt(@NonNull String ext) {
        return getTempFile(UUID.randomUUID().toString(), ext);
    }

    public static class ZkPathBuilder {
        private final List<String> paths = new ArrayList<>();

        public ZkPathBuilder() {
        }

        public ZkPathBuilder(@NonNull String base) {
            paths.add(base);
        }

        public ZkPathBuilder withPath(@NonNull String path) {
            paths.add(path);
            return this;
        }

        public String build() {
            if (!paths.isEmpty()) {
                StringBuilder fmt = new StringBuilder();
                for (String path : paths) {
                    fmt.append("/").append(path);
                }
                return formatZkPath(fmt.toString());
            }
            return null;
        }

        public static String formatZkPath(@NonNull String path) {
            path = path.replaceAll("/\\s*/", "/");
            if (path.endsWith("/")) {
                path = path.substring(0, path.length() - 2);
            }
            return path;
        }
    }

    public static File readFile(@NonNull String path) throws Exception {
        URI uri = new URI(path);
        String schema = uri.getScheme();
        File file = null;
        if (Strings.isNullOrEmpty(schema)) {
            file = new File(path);
            if (!file.exists()) {
                throw new IOException(String.format("Specified path not found. [path=%s]",
                        file.getAbsolutePath()));
            }
        } else if (schema.compareToIgnoreCase("file") == 0) {
            path = String.format("%s/%s", uri.getHost(), uri.getPath());
            file = new File(path);
            if (!file.exists()) {
                throw new IOException(String.format("Specified path not found. [path=%s]",
                        file.getAbsolutePath()));
            }
        } else {
            URL url = uri.toURL();
            String ext = FilenameUtils.getExtension(path);
            String name = UUID.randomUUID().toString();
            if (Strings.isNullOrEmpty(ext))
                file = getTempFile(name);
            else
                file = getTempFile(name, ext);
            try (InputStream in = url.openStream()) {
                ReadableByteChannel rbc = Channels.newChannel(in);
                try (FileOutputStream fos = new FileOutputStream(file)) {
                    fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
                }
            }
        }
        return file;
    }
}
