package ai.sapper.hcdc.common.schema;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class SchemaVersion {
    private int majorVersion = 0;
    private int minorVersion = 1;

    public String path() {
        return String.format("%d/%d", majorVersion, minorVersion);
    }

    @Override
    public String toString() {
        return String.format("%d.%d", majorVersion, minorVersion);
    }
}
