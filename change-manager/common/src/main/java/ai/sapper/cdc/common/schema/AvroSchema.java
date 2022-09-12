package ai.sapper.cdc.common.schema;

import ai.sapper.cdc.common.utils.ChecksumUtils;
import ai.sapper.cdc.common.utils.DefaultLogger;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.avro.Schema;
import org.slf4j.event.Level;

import java.util.List;

@Getter
@Setter
public class AvroSchema {
    private SchemaVersion version;
    private String hash;
    private String schemaStr;
    private String zkPath;
    @JsonIgnore
    private Schema schema;

    public AvroSchema withSchema(@NonNull Schema schema) throws Exception {
        schemaStr = schema.toString(false);
        hash = ChecksumUtils.generateHash(schemaStr.replaceAll("\\s", ""));
        this.schema = schema;
        return this;
    }

    public AvroSchema withSchemaStr(@NonNull String schemaStr) throws Exception {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(schemaStr));
        this.schemaStr = schemaStr;
        load();
        hash = ChecksumUtils.generateHash(schemaStr);
        return this;
    }

    public AvroSchema load() throws Exception {
        Preconditions.checkState(!Strings.isNullOrEmpty(schemaStr));
        schema = new Schema.Parser().parse(schemaStr);
        return this;
    }

    public boolean compare(@NonNull AvroSchema target)
            throws Exception {
        Preconditions.checkNotNull(this.schema);
        Preconditions.checkNotNull(target.schema);
        if (hash.compareTo(target.hash) != 0) {
            return false;
        }
        List<SchemaEvolutionValidator.Message> response =
                SchemaEvolutionValidator
                        .checkBackwardCompatibility(this.schema,
                                target.schema,
                                this.schema.getName());
        if (response.isEmpty()) return true;
        Level maxLevel = Level.DEBUG;
        for (SchemaEvolutionValidator.Message message : response) {
            if (DefaultLogger.isGreaterOrEqual(message.getLevel(), maxLevel)) {
                maxLevel = message.getLevel();
            }
        }
        return DefaultLogger.isGreaterOrEqual(Level.DEBUG, maxLevel);
    }
}
