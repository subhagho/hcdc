package ai.sapper.hcdc.agents.common;

import ai.sapper.cdc.common.utils.DefaultLogger;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DFSEditsFileFinder {
    @Getter
    @Setter
    @Accessors(fluent = true)
    @ToString
    public static class EditsLogFile {
        private String path;
        private long startTxId = -1;
        private long endTxId = -1;
    }

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
                        DefaultLogger.LOGGER.debug(
                                String.format("[START TX ID=%d] [file=%s]", txid, file.getAbsolutePath()));
                        return file.getAbsolutePath();
                    }
                }
            }
        }
        return null;
    }

    public static List<EditsLogFile> findEditsFiles(@NonNull String rootPath, long startTx, long endTx) throws IOException {
        File dir = new File(rootPath);
        if (!dir.exists()) {
            throw new IOException(String.format("Specified root directory not found. [path=%s]", dir.getAbsolutePath()));
        }
        if (!dir.canRead()) {
            throw new IOException(String.format("Error reading specified root directory. [path=%s]", dir.getAbsolutePath()));
        }
        Pattern pattern = Pattern.compile(REGEX_EDIT_LOG_FILE);
        File[] files = dir.listFiles();
        List<EditsLogFile> paths = new ArrayList<>();
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
                        Preconditions.checkState(etx >= stx);
                        if (validStart(startTx, endTx, stx) || validEnd(startTx, endTx, etx)) {
                            EditsLogFile ef = new EditsLogFile();
                            ef.path = file.getAbsolutePath();
                            ef.startTxId = stx;
                            ef.endTxId = etx;

                            paths.add(ef);
                        }
                    }
                }
            }
        }
        if (!paths.isEmpty()) {
            paths.sort(new Comparator<EditsLogFile>() {
                @Override
                public int compare(EditsLogFile o1, EditsLogFile o2) {
                    return (int) (o1.startTxId - o2.startTxId);
                }
            });
            return paths;
        }
        return null;
    }

    private static boolean validStart(long tStart, long tEnd, long fStart) {
        return (fStart >= tStart || tStart < 0) && (fStart <= tEnd || tEnd < 0);
    }

    private static boolean validEnd(long tStart, long tEnd, long fEnd) {
        return (fEnd >= tStart || tStart < 0) && (fEnd <= tEnd || tEnd < 0);
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
            return Long.parseLong(s.trim());
        }
    }

    public static EditsLogFile parseFileName(@NonNull String filename) throws IOException {
        File file = new File(filename);
        String name = file.getName();
        Pattern pattern = Pattern.compile(REGEX_EDIT_LOG_FILE);
        Matcher m = pattern.matcher(name);
        if (m.matches()) {
            String s = m.group(1);
            String e = m.group(2);
            if (!Strings.isNullOrEmpty(s) && !Strings.isNullOrEmpty(e)) {
                long stx = Long.parseLong(s);
                long etx = Long.parseLong(e);
                EditsLogFile ef = new EditsLogFile();
                ef.path = file.getAbsolutePath();
                ef.startTxId = stx;
                ef.endTxId = etx;

                return ef;
            }
        }
        return null;
    }
}
