package ai.sapper.cdc.core.connections;

import lombok.NonNull;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;

import java.io.File;

public class TestUtils {

    public static XMLConfiguration readFile(@NonNull String configFile) throws Exception {
        File cf = new File(configFile);
        if (!cf.exists()) {
            throw new Exception(String.format("Configuration file not found. ]path=%s]", cf.getAbsolutePath()));
        }
        Configurations configs = new Configurations();
        return configs.xml(cf);
    }
}
