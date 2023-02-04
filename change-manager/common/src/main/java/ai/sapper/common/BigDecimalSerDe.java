package ai.sapper.common;

import ai.sapper.common.model.BDecimal;
import ai.sapper.common.model.BInteger;
import lombok.NonNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;

public class BigDecimalSerDe {
    public static BDecimal write(@NonNull BigDecimal val) {
        BInteger iv = BigIntegerSerDe.write(val.unscaledValue());
        BDecimal.Builder builder = BDecimal.newBuilder();
        builder.setValue(iv)
                .setScale(val.scale())
                .setPrecision(val.precision());

        return builder.build();
    }

    public static BigDecimal read(@NonNull BDecimal message) {
        BigInteger iv = BigIntegerSerDe.read(message.getValue());
        MathContext mc = new MathContext(message.getPrecision());
        return new BigDecimal(iv, message.getScale(), mc);
    }
}
