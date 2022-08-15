package ai.sapper.cdc.common.model.services;

import lombok.NonNull;

public enum EConfigFileType {
    File, Resource, Remote;

    public static EConfigFileType parse(@NonNull String value) {
        for(EConfigFileType t : EConfigFileType.values()) {
            if (t.name().compareToIgnoreCase(value) == 0) {
                return t;
            }
        }
        return null;
    }
}
