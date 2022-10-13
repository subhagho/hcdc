package ai.sapper.cdc.common.schema;

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
public class SchemaVersion {
    private int majorVersion = 0;
    private int minorVersion = 1;

    public SchemaVersion() {
    }

    public SchemaVersion(@NonNull SchemaVersion sv) {
        this.majorVersion = sv.majorVersion;
        this.minorVersion = sv.minorVersion;
    }

    public SchemaVersion(int majorVersion, int minorVersion) {
        Preconditions.checkArgument(majorVersion >= 0);
        Preconditions.checkArgument(minorVersion >= 0);
        this.majorVersion = majorVersion;
        this.minorVersion = minorVersion;
    }

    public String path() {
        return String.format("%d/%d", majorVersion, minorVersion);
    }

    @Override
    public String toString() {
        return String.format("%d.%d", majorVersion, minorVersion);
    }

    @Override
    public boolean equals(Object that) {
        if (that instanceof SchemaVersion) {
            SchemaVersion t = (SchemaVersion) that;
            return (majorVersion == t.majorVersion && minorVersion == t.minorVersion);
        }
        return false;
    }

    public int compare(SchemaVersion target) {
        if (target == null) {
            return Integer.MIN_VALUE;
        }
        int ret = target.majorVersion - majorVersion;
        if (ret == 0) {
            ret = target.minorVersion - minorVersion;
        }
        return ret;
    }
}
