package ai.sapper.cdc.core.utils;

import ai.sapper.cdc.core.io.FileSystem;
import ai.sapper.cdc.core.io.impl.local.LocalFileSystem;
import ai.sapper.cdc.core.io.impl.s3.S3FileSystem;
import ai.sapper.hcdc.common.model.DFSChangeData;
import lombok.NonNull;

public class HFSHelper {

    public static DFSChangeData.FileSystemCode fileSystemCode(@NonNull FileSystem fs) throws Exception {
        if (fs.getClass().equals(LocalFileSystem.class)) {
            return DFSChangeData.FileSystemCode.LOCAL;
        } else if (fs.getClass().equals(S3FileSystem.class)) {
            return DFSChangeData.FileSystemCode.S3;
        }
        throw new Exception(String.format("FileSystem not recognized: [type=%s]", fs.getClass().getCanonicalName()));
    }
}
