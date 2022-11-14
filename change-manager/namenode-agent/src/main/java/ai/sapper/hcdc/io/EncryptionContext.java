package ai.sapper.hcdc.io;

import ai.sapper.cdc.common.model.Context;

public class EncryptionContext extends Context {
    public static final String CONTEXT_FILE_PATH = "hdfs.path";
    public static final String CONTEXT_BLOCK_ID = "hdfs.block.id";
    public static final String CONTEXT_BLOCK_NAME = "hdfs.block.name";
}
