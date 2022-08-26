package ai.sapper.cdc.common.model;

public class AvroChangeType {
    public static int OP_SCHEMA_CREATE = 0;
    public static int OP_SCHEMA_UPDATE = 1;
    public static int OP_SCHEMA_DELETE = 2;
    public static int OP_DATA_INSERT = 3;
    public static int OP_DATA_UPDATE = 4;
    public static int OP_DATA_DELETE = 5;
}
