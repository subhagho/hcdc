package ai.sapper.hcdc.agents.pipeline.impl;

import ai.sapper.cdc.core.NameNodeEnv;
import ai.sapper.cdc.core.messaging.kafka.KafkaOffset;
import ai.sapper.hcdc.agents.pipeline.EntityChangeDeltaProcessor;
import lombok.NonNull;

public class KafkaEntityChangeDeltaProcessor extends EntityChangeDeltaProcessor<KafkaOffset> {
    public KafkaEntityChangeDeltaProcessor(@NonNull NameNodeEnv env, @NonNull String name) {
        super(env, name);
    }
}
