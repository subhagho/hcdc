package ai.sapper.cdc.core.model;

import lombok.NonNull;

public enum EFileType {
    UNKNOWN,
    CSV,
    PARQUET,
    AVRO,
    JSON,
    ORC;

    public static EFileType parse(@NonNull String value) {
        for (EFileType type : EFileType.values()) {
            if (type.name().compareToIgnoreCase(value) == 0) {
                return type;
            }
        }
        return UNKNOWN;
    }
}
