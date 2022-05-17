package ai.sapper.hcdc.core;

import lombok.NonNull;
import org.apache.commons.lang3.SerializationException;

import java.nio.ByteBuffer;

public interface MessageSerDe<T> {
    ByteBuffer serialize(@NonNull T message) throws SerializationException;

    T deserialize(@NonNull ByteBuffer data) throws SerializationException;
}
