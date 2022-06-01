package ai.sapper.hcdc.agents.namenode.model;

import ai.sapper.hcdc.agents.namenode.DFSAgentError;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
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

    private final Map<String, DFSFileTnx> upserts = new HashMap<>();
    private final Map<String, DFSFileTnx> deletes = new HashMap<>();

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

    public boolean checkAndSetTxnId(long txnId) {
        Preconditions.checkArgument(txnId >= 0);
        boolean ret = false;
        if (txnId > endTnxId) {
            endTnxId = txnId;
            ret = true;
        }
        return ret;
    }

    public DFSFileTnx get(String path) {
        return upserts.get(path);
    }

    public boolean containsFile(String path) {
        return upserts.containsKey(path);
    }

    public void add(@NonNull DFSFileTnx fileTnx, boolean delete) throws DFSAgentError {
        if (!delete) {
            if (upserts.containsKey(fileTnx.getPath())) {
                if (!deletes.containsKey(fileTnx.getPath())) {
                    throw new DFSAgentError(String.format("Attempt to update/insert duplicate file record. [path=%s]", fileTnx.getPath()));
                }
                DFSFileTnx fi = upserts.get(fileTnx.getPath());
                DFSFileTnx df = deletes.get(fileTnx.getPath());
                if (df.getStartTnxId() < fi.getEndTnxId()) {
                    throw new DFSAgentError(String.format("Attempt to update/insert duplicate file record. [path=%s]", fileTnx.getPath()));
                }
            }
            upserts.put(fileTnx.getPath(), fileTnx);
        } else {
            deletes.put(fileTnx.getPath(), fileTnx);
        }
    }
}
