package ai.sapper.hcdc.common.schema;

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
}
