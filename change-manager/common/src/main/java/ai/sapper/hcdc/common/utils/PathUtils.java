package ai.sapper.hcdc.common.utils;

import lombok.NonNull;

public class PathUtils {
    public static String formatZkPath(@NonNull String path) {
        return path.replaceAll("/\\s*/", "/");
    }
}
