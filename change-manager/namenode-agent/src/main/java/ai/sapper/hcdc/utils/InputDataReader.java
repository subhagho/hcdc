package ai.sapper.hcdc.utils;

import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

@Getter
@Accessors(fluent = true)
public abstract class InputDataReader<T> {
    public enum EInputFormat {
        CVS;

        public static boolean isValidFile(@NonNull String filename) {
            String[] parts = filename.split("\\.");
            if (parts.length > 1) {
                String ext = parts[parts.length - 1];
                for (EInputFormat f : EInputFormat.values()) {
                    if (f.name().compareToIgnoreCase(ext) == 0) return true;
                }
            }
            return false;
        }

        public static EInputFormat parse(@NonNull String value) {
            for (EInputFormat f : EInputFormat.values()) {
                if (f.name().compareToIgnoreCase(value) == 0) return f;
            }
            return null;
        }
    }

    private final EInputFormat dataType;
    private final String filename;
    private final List<T> records = new ArrayList<>();
    private final Map<String, Integer> header = new HashMap<>();

    public InputDataReader(@NonNull String filename, @NonNull InputDataReader.EInputFormat dataType) {
        this.filename = filename;
        this.dataType = dataType;
    }

    public String getFilePath() throws IOException {
        Path path = Paths.get(filename);
        BasicFileAttributes attrs = Files.readAttributes(path.toAbsolutePath(), BasicFileAttributes.class);

        FileTime ft = attrs.lastModifiedTime();
        Date dt = new Date(ft.toMillis());
        DateFormat df = new SimpleDateFormat("yyyy/MM/dd/HH");

        return df.format(dt);
    }

    public abstract void read() throws IOException;
}
