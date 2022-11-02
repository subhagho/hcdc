package ai.sapper.cdc.core.connections.settngs;

public enum EConnectionType {
    kafka, zookeeper, db, rest, hadoop, debezium, others;

    public static EConnectionType parse(String name) {
        for (EConnectionType type : EConnectionType.values()) {
            if (type.name().compareToIgnoreCase(name) == 0) {
                return type;
            }
        }
        return null;
    }
}
