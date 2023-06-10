package ai.sapper.hcdc.agents.pipeline.impl;

import ai.sapper.cdc.core.NameNodeEnv;
import ai.sapper.cdc.core.messaging.kafka.KafkaOffset;
import ai.sapper.hcdc.agents.pipeline.EntityChangeDeltaReader;
import lombok.NonNull;

public class KafkaEntityChangeDeltaReader extends EntityChangeDeltaReader<KafkaOffset> {
    public KafkaEntityChangeDeltaReader(@NonNull NameNodeEnv env) {
        super(env);
    }
}
