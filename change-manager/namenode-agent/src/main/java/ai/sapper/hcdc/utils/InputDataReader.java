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
    public enum EDataType {
        CVS
    }

    private final EDataType dataType;
    private final String filename;
    private final List<T> records = new ArrayList<>();
    private final Map<String, Integer> header = new HashMap<>();

    public InputDataReader(@NonNull String filename, @NonNull EDataType dataType) {
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
