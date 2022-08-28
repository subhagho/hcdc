package ai.sapper.cdc.core.io.impl.local;

import lombok.NonNull;
import org.apache.commons.io.filefilter.IOFileFilter;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LocalDirFilter implements IOFileFilter {
    private final String regex;
    private final Pattern pattern;

    public LocalDirFilter(@NonNull String regex) {
        this.regex = regex;
        this.pattern = Pattern.compile(regex);
    }

    /**
     * Checks to see if the File should be accepted by this filter.
     * <p>
     * Defined in {@link FileFilter}.
     * </p>
     *
     * @param file the File to check.
     * @return true if this file matches the test.
     */
    @Override
    public boolean accept(File file) {
        String path = file.getAbsolutePath();
        if (file.isFile()) {
            path = file.getParentFile().getAbsolutePath();
        }
        Matcher m = pattern.matcher(path);
        return m.matches();
    }

    /**
     * Checks to see if the File should be accepted by this filter.
     * <p>
     * Defined in {@link FilenameFilter}.
     * </p>
     *
     * @param dir  the directory File to check.
     * @param name the file name within the directory to check.
     * @return true if this file matches the test.
     */
    @Override
    public boolean accept(File dir, String name) {
        Matcher m = pattern.matcher(dir.getAbsolutePath());
        return m.matches();
    }
}
