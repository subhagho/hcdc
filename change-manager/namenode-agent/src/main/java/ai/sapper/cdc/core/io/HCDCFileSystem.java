package ai.sapper.cdc.core.io;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.hcdc.common.model.DFSChangeData;
import ai.sapper.cdc.common.model.SchemaEntity;
import ai.sapper.cdc.core.model.DFSBlockState;
import ai.sapper.cdc.core.model.DFSFileState;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public abstract class HCDCFileSystem extends FileSystem {

    public abstract DFSChangeData.FileSystemCode fileSystemCode();

}
