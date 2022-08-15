package ai.sapper.cdc.common.utils;

import lombok.NonNull;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

public class PathUtils {
    public static final String TEMP_PATH = String.format("%s/hcdc/tmp", System.getProperty("java.io.tmpdir"));

    public static String formatZkPath(@NonNull String path) {
        path = path.replaceAll("/\\s*/", "/");
        if (path.endsWith("/")) {
            path = path.substring(0, path.length() - 2);
        }
        return path;
    }

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


}
