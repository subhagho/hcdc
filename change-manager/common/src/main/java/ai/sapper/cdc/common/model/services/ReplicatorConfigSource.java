package ai.sapper.cdc.common.model.services;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class ReplicatorConfigSource extends ConfigSource {
    private String fsImagePath;
    private String tmpDir;
}
