package ai.sapper.cdc.core.io;

import ai.sapper.cdc.common.model.SchemaEntity;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.Closeable;
import java.io.IOException;

@Getter
@Accessors(fluent = true)
public abstract class Archiver implements Closeable {
    private FileSystem targetFS;

    public abstract void init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                              String pathPrefix) throws IOException;

    public abstract void archive(@NonNull FSFile source,
                                 @NonNull PathInfo target,
                                 @NonNull FileSystem sourceFS) throws IOException;

    public abstract PathInfo getTargetPath(@NonNull String path,
                                       @NonNull SchemaEntity schemaEntity);
}
