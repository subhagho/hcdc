package ai.sapper.cdc.core.utils;

import lombok.NonNull;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FileSystemUtils {
    public static List<Path> list(@NonNull String path, @NonNull FileSystem fs) throws IOException {
        Path hp = new Path(path);
        if (fs.exists(hp)) {
            List<Path> paths = new ArrayList<>();
            RemoteIterator<LocatedFileStatus> files = fs.listFiles(hp, true);
            if (files != null) {
                while (files.hasNext()) {
                    LocatedFileStatus file = files.next();
                    if (file.isFile()) {
                        paths.add(file.getPath());
                    }
                }
            }
            if (!paths.isEmpty()) return paths;
        }
        return null;
    }
}
