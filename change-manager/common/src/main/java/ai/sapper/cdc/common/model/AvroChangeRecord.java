package ai.sapper.cdc.common.model;

import lombok.Getter;
import lombok.Setter;
import org.apache.avro.generic.GenericRecord;

@Getter
@Setter
public class AvroChangeRecord {
    private long txId;
    private int op;
    private long timestamp;
    private GenericRecord data;
}
