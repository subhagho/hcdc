package ai.sapper.cdc.common.model;

import lombok.Getter;
import lombok.experimental.Accessors;

public class AvroChangeType {
    private static final int OP_SCHEMA_CREATE = 0;
    private static final int OP_SCHEMA_UPDATE = 1;
    private static final int OP_SCHEMA_DELETE = 2;
    private static final int OP_DATA_INSERT = 3;
    private static final int OP_DATA_UPDATE = 4;
    private static final int OP_DATA_DELETE = 5;

    @Getter
    @Accessors(fluent = true)
    public enum EChangeType {
        EntityCreate(OP_SCHEMA_CREATE),
        EntityUpdate(OP_SCHEMA_UPDATE),
        EntityDelete(OP_SCHEMA_DELETE),
        RecordInsert(OP_DATA_INSERT),
        RecordUpdate(OP_DATA_UPDATE),
        RecordDelete(OP_DATA_DELETE);

        private final int opCode;

        EChangeType(int opCode) {
            this.opCode = opCode;
        }

        public static boolean isSchemaChange(EChangeType type) {
            return (type == EntityCreate || type == EntityDelete || type == EntityUpdate);
        }
    }
}
