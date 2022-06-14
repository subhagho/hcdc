package ai.sapper.hcdc.common.utils;

import lombok.NonNull;

import java.io.File;

public class PathUtils {
    public static String formatZkPath(@NonNull String path) {
        return path.replaceAll("/\\s*/", "/");
    }
}
