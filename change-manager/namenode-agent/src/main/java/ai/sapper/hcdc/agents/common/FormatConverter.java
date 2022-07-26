package ai.sapper.hcdc.agents.common;

import lombok.NonNull;

import java.io.File;
import java.io.IOException;

public interface FormatConverter {
    boolean canParse(@NonNull File path) throws IOException;

    void convert(@NonNull File source, @NonNull File output) throws IOException;

    boolean supportsPartial();
}
