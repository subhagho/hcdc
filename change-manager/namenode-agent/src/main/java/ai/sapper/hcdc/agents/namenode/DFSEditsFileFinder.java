package ai.sapper.hcdc.agents.namenode;

import ai.sapper.hcdc.common.utils.DefaultLogger;
import com.google.common.base.Strings;
import lombok.NonNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DFSEditsFileFinder {
    public static final String REGEX_CURRENT_FILE = "edits_inprogress_(\\d+$)";
    public static final String REGEX_EDIT_LOG_FILE = "edits_(\\d+)-(\\d+$)";
    public static final String FILE_SEEN_TXID = "seen_txid";

    public static String getCurrentEditsFile(@NonNull String rootPath) throws IOException {
        File dir = new File(rootPath);
        if (!dir.exists()) {
            throw new IOException(String.format("Specified root directory not found. [path=%s]", dir.getAbsolutePath()));
        }
        if (!dir.canRead()) {
            throw new IOException(String.format("Error reading specified root directory. [path=%s]", dir.getAbsolutePath()));
        }
        Pattern pattern = Pattern.compile(REGEX_CURRENT_FILE);
        File[] files = dir.listFiles();
        if (files != null && files.length > 0) {
            for (File file : files) {
                String name = file.getName();
                Matcher m = pattern.matcher(name);
                if (m.matches()) {
                    String s = m.group(1);
                    if (!Strings.isNullOrEmpty(s)) {
                        long txid = Long.parseLong(s);
                        DefaultLogger.LOG.debug(String.format("[START TX ID=%d] [file=%s]", txid, file.getAbsolutePath()));
                        return file.getAbsolutePath();
                    }
                }
            }
        }
        return null;
    }

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

    public static long findSeenTxID(@NonNull String rootPath) throws IOException {
        File file = new File(String.format("%s/%s", rootPath, FILE_SEEN_TXID));
        if (!file.exists()) {
            throw new IOException(String.format("SEEN TX file not found. [path=%s]", file.getAbsolutePath()));
        }
        try (FileInputStream fis = new FileInputStream(file)) {
            byte[] data = new byte[256];
            int r = fis.read(data);
            if (r < 0) {
                throw new IOException(String.format("SEEN TX file is empty. [path=%s]", file.getAbsolutePath()));
            }
            String s = new String(data, 0, r, StandardCharsets.UTF_8);
            if (Strings.isNullOrEmpty(s)) {
                throw new IOException(String.format("Empty Seen TX data. [path=%s]", file.getAbsolutePath()));
            }
            return Long.parseLong(s);
        }
    }
}
