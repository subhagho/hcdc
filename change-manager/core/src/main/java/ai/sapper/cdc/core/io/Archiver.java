package ai.sapper.cdc.core.io;

import ai.sapper.cdc.common.schema.SchemaEntity;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

@Getter
@Accessors(fluent = true)
public abstract class Archiver implements Closeable {
    public static final String CONFIG_ARCHIVER = "archiver";
    public static final String CONFIG_KEY_ARCHIVE = "archived";

    private FileSystem targetFS;

    public abstract void init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                              String pathPrefix) throws IOException;

    public abstract PathInfo archive(@NonNull PathInfo source,
                                     @NonNull PathInfo target,
                                     @NonNull FileSystem sourceFS) throws IOException;

    public abstract PathInfo getTargetPath(@NonNull String path,
                                           @NonNull SchemaEntity schemaEntity);

    public abstract File getFromArchive(@NonNull PathInfo path) throws IOException;
}
