package ai.sapper.cdc.core.connections.settngs;

import ai.sapper.cdc.common.utils.DefaultLogger;
import com.google.common.base.Strings;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.List;

public class KafkaPartitionsParser implements SettingParser<List<Integer>> {
    @Override
    public List<Integer> parse(@NonNull String value) throws Exception {
        List<Integer> partitions = new ArrayList<>();
        if (!Strings.isNullOrEmpty(value)) {
            if (value.indexOf(';') >= 0) {
                String[] parts = value.split(";");
                for (String part : parts) {
                    if (Strings.isNullOrEmpty(part)) continue;
                    Integer p = Integer.parseInt(part.trim());
                    partitions.add(p);
                    DefaultLogger.LOGGER.debug(String.format("Added partition; [%d]", p));
                }
            } else {
                Integer p = Integer.parseInt(value.trim());
                partitions.add(p);
                DefaultLogger.LOGGER.debug(String.format("Added partition; [%d]", p));
            }
        }
        if (partitions.isEmpty()) partitions.add(0);
        return partitions;
    }

    @Override
    public String serialize(@NonNull Object source) throws Exception {
        List<Integer> value = (List<Integer>) source;
        StringBuilder builder = new StringBuilder();
        boolean first = true;
        for (int v : value) {
            if (first) first = false;
            else {
                builder.append(";");
            }
            builder.append(v);
        }
        return builder.toString();
    }
}
