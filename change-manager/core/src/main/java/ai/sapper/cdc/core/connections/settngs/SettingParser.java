package ai.sapper.cdc.core.connections.settngs;

import lombok.NonNull;

public interface SettingParser<T> {
    T parse(@NonNull String value) throws Exception;

    String serialize(@NonNull Object value) throws Exception;

    class NullSettingParser implements SettingParser<Void> {

        @Override
        public Void parse(@NonNull String value) throws Exception {
            throw new Exception("Method should not be called...");
        }

        @Override
        public String serialize(@NonNull Object value) throws Exception {
            throw new Exception("Method should not be called...");
        }
    }

    public static final Class<? extends SettingParser<?>> NULL = NullSettingParser.class;
}
