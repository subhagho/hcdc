package ai.sapper.common;

import ai.sapper.common.model.BInteger;
import com.google.protobuf.ByteString;
import lombok.NonNull;

import java.math.BigInteger;

public class BigIntegerSerDe {
    public static BInteger write(@NonNull BigInteger val) {
        BInteger.Builder builder = BInteger.newBuilder();
        ByteString bytes = ByteString.copyFrom(val.toByteArray());
        builder.setValue(bytes);
        return builder.build();
    }

    public static BigInteger read(@NonNull BInteger message) {
        ByteString bytes = message.getValue();
        return new BigInteger(bytes.toByteArray());
    }
}
