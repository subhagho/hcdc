package ai.sapper.hcdc.agents.namenode;

import com.google.common.base.Strings;
import lombok.NonNull;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DFSEditsFileFinder {
    public static final String REGEX_CURRENT_FILE = "edits_inprogress_(\\d+$)";
    public static final String REGEX_EDIT_LOG_FILE = "edits_(\\d+)-(\\d+$)";

    public static List<String> findEditsFiles(@NonNull String rootPath, long startTx, long endTx) throws IOException {
        File dir = new File(rootPath);
        if (!dir.exists()) {
            throw new IOException(String.format("Specified root directory not found. [path=%s]", dir.getAbsolutePath()));
        }
        if (!dir.canRead()) {
            throw new IOException(String.format("Error reading specified root directory. [path=%s]", dir.getAbsolutePath()));
        }
        Pattern pattern = Pattern.compile(REGEX_EDIT_LOG_FILE);
        File[] files = dir.listFiles();
        List<String> paths = new ArrayList<>();
        if (files != null && files.length > 0) {
            for (File file : files) {
                String name = file.getName();
                Matcher m = pattern.matcher(name);
                if (m.matches()) {
                    String s = m.group(1);
                    String e = m.group(2);
                    if (!Strings.isNullOrEmpty(s) && !Strings.isNullOrEmpty(e)) {
                        long stx = Long.parseLong(s);
                        long etx = Long.parseLong(e);
                        if ((stx >= startTx || startTx < 0) && (etx <= endTx || endTx < 0)) {
                            paths.add(file.getAbsolutePath());
                        }
                    }
                }
            }
        }
        if (!paths.isEmpty()) return paths;
        return null;
    }
}
