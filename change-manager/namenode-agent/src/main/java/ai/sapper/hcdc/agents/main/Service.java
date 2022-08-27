package ai.sapper.hcdc.agents.main;

import ai.sapper.cdc.common.AbstractState;
import ai.sapper.cdc.common.model.services.EConfigFileType;
import ai.sapper.hcdc.agents.common.NameNodeEnv;
import lombok.NonNull;

public interface Service<E extends Enum<?>> {
    Service<E> setConfigFile(@NonNull String path);

    Service<E> setConfigSource(@NonNull String type);

    Service<E> init() throws Exception;

    Service<E> start() throws Exception;

    Service<E> stop() throws Exception;

    AbstractState<E> status();

    String name();
}
