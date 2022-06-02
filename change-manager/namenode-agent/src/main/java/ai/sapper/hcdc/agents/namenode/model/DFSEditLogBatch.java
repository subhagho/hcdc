package ai.sapper.hcdc.agents.namenode.model;

import ai.sapper.hcdc.agents.namenode.DFSAgentError;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Getter
@Setter
public class DFSEditLogBatch {

    private static final String REGEX_CURRENT_FILE = "edits_inprogress_(\\d+$)";
    private static final String REGEX_EDIT_LOG_FILE = "edits_(\\d+)-(\\d+$)";
    private final String filename;

    private long version;
    private long startTnxId = Long.MAX_VALUE;
    private long endTnxId = -1;
    private boolean isCurrent;
    private final List<DFSLogTransaction<?>> transactions = new ArrayList<>();

    public DFSEditLogBatch(@NonNull String filename) {
        this.filename = filename;
    }

    public void setup() throws IOException {
        File inf = new File((filename));
        if (!inf.exists()) {
            throw new IOException(String.format("DFS Edit Log: File not found. [path=%s]", inf.getAbsolutePath()));
        }
        String name = inf.getName();
        Pattern pattern = Pattern.compile(REGEX_CURRENT_FILE);
        Matcher matcher = pattern.matcher(name);
        if (matcher.matches()) {
            String sTnxId = matcher.group(1);
            if (Strings.isNullOrEmpty(sTnxId)) {
                throw new IOException(String.format("Error extracting start transaction ID. [string=%s]", name));
            }
            startTnxId = Long.parseLong(sTnxId);
            isCurrent = true;
        } else {
            pattern = Pattern.compile(REGEX_EDIT_LOG_FILE);
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
