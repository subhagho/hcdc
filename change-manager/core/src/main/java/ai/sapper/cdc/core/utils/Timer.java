package ai.sapper.cdc.core.utils;

import io.micrometer.core.instrument.DistributionSummary;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.Closeable;
import java.io.IOException;

@Getter
@Accessors(fluent = true)
public class Timer implements Closeable {
    private final DistributionSummary summary;
    private long startTime;

    public Timer(@NonNull DistributionSummary summary) {
        this.summary = summary;
        startTime = System.currentTimeMillis();
    }

    @Override
    public void close() throws IOException {
        summary.record(System.currentTimeMillis() - startTime);
    }
}
