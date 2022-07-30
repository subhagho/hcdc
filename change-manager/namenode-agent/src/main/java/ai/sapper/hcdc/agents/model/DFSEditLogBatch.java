package ai.sapper.hcdc.agents.model;

import ai.sapper.hcdc.agents.common.DFSEditsFileFinder;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Getter
@Setter
@Accessors(fluent = true)
public class DFSEditLogBatch {
    private final String filename;

    private long version;
    private long startTnxId = Long.MAX_VALUE;
    private long endTnxId = -1;
    private boolean isCurrent;
    private final List<DFSTransactionType<?>> transactions = new ArrayList<>();

    public DFSEditLogBatch(@NonNull String filename) {
        this.filename = filename;
    }

    public DFSEditLogBatch(@NonNull DFSEditLogBatch batch) {
        this.filename = batch.filename;
        this.version = batch.version;
        this.startTnxId = batch.startTnxId();
        this.endTnxId = batch.endTnxId();
        this.isCurrent = batch.isCurrent;
    }

    public void setup() throws IOException {
        File inf = new File((filename));
        if (!inf.exists()) {
            throw new IOException(String.format("DFS Edit Log: File not found. [path=%s]", inf.getAbsolutePath()));
        }
        String name = inf.getName();
        Pattern pattern = Pattern.compile(DFSEditsFileFinder.REGEX_CURRENT_FILE);
        Matcher matcher = pattern.matcher(name);
        if (matcher.matches()) {
            String sTnxId = matcher.group(1);
            if (Strings.isNullOrEmpty(sTnxId)) {
                throw new IOException(String.format("Error extracting start transaction ID. [string=%s]", name));
            }
            startTnxId = Long.parseLong(sTnxId);
            isCurrent = true;
        } else {
            pattern = Pattern.compile(DFSEditsFileFinder.REGEX_EDIT_LOG_FILE);
            matcher = pattern.matcher(name);
            if (!matcher.matches()) {
                throw new IOException(String.format("Invalid Edit Log file. [name=%s][path=%s]", name, inf.getAbsolutePath()));
            }
            String sTnxId = matcher.group(1);
            if (Strings.isNullOrEmpty(sTnxId)) {
                throw new IOException(String.format("Error extracting start transaction ID. [string=%s]", name));
            }
            startTnxId = Long.parseLong(sTnxId);
            String eTnxId = matcher.group(2);
            if (Strings.isNullOrEmpty(eTnxId)) {
                throw new IOException(String.format("Error extracting end transaction ID. [string=%s]", name));
            }
            endTnxId = Long.parseLong(eTnxId);
            isCurrent = false;
        }
    }

}
