package ai.sapper.cdc.core.connections.settngs;

import ai.sapper.cdc.common.utils.DefaultLogger;
import lombok.NonNull;
import org.apache.commons.configuration2.ex.ConfigurationException;

public class HdfsUrlParser implements SettingParser<String[][]> {
    @Override
    public String[][] parse(@NonNull String value) throws Exception {
        String[][] r = new String[2][2];
        String[] nns = value.split(";");
        if (nns.length != 2) {
            throw new ConfigurationException(
                    String.format("Invalid NameNode(s) specified. Expected count = 2, specified = %d",
                            nns.length));
        }
        for (int ii = 0; ii < nns.length; ii++) {
            String n = nns[ii];
            String[] parts = n.split("=");
            if (parts.length != 2) {
                throw new ConfigurationException(
                        String.format("Invalid NameNode specified. Expected count = 2, specified = %d",
                                parts.length));
            }
            String key = parts[0].trim();
            String address = parts[1].trim();

            DefaultLogger.LOGGER.info(String.format("Registering namenode [%s -> %s]...", key, address));
            r[ii][0] = key;
            r[ii][1] = address;
        }
        return r;
    }

    @Override
    public String serialize(@NonNull Object source) throws Exception {
        String[][] value = (String[][]) source;
        StringBuilder builder = new StringBuilder();
        boolean first = true;
        for (String[] nn : value) {
            if (first) first = false;
            else {
                builder.append(";");
            }
            if (nn.length != 2) {
                throw new Exception(String.format("Invalid URL definition: [url=%s]", nn));
            }
            builder.append(nn[0]).append("=").append(nn[1]);
        }
        return builder.toString();
    }
}
