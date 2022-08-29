package ai.sapper.cdc.core.io;

import ai.sapper.hcdc.common.model.DFSChangeData;
import lombok.Getter;
import lombok.experimental.Accessors;

@Getter
@Accessors(fluent = true)
public abstract class CDCFileSystem extends FileSystem {

    public abstract DFSChangeData.FileSystemCode fileSystemCode();

}
