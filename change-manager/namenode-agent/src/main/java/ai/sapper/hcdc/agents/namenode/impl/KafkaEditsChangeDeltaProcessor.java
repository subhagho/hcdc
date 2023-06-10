package ai.sapper.hcdc.agents.namenode.impl;

import ai.sapper.cdc.core.NameNodeEnv;
import ai.sapper.cdc.core.messaging.kafka.KafkaOffset;
import ai.sapper.hcdc.agents.namenode.EditsChangeDeltaProcessor;
import lombok.NonNull;

public class KafkaEditsChangeDeltaProcessor extends EditsChangeDeltaProcessor<KafkaOffset> {
    public KafkaEditsChangeDeltaProcessor(@NonNull NameNodeEnv env, @NonNull String name) {
        super(env, name);
    }
}
