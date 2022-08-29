package ai.sapper.cdc.common.model.services;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class ReplicatorConfigSource extends ConfigSource {
    private String fsImageDir;
    private String tmpDir;
}
