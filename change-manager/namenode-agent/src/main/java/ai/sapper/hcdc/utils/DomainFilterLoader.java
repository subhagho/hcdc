package ai.sapper.hcdc.utils;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.common.model.services.EConfigFileType;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.filters.DomainManager;
import ai.sapper.hcdc.agents.common.NameNodeEnv;
import ai.sapper.hcdc.agents.common.ProcessorStateManager;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.parquet.Strings;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

@Getter
@Setter
public class DomainFilterLoader {
    @Parameter(names = {"--config", "-c"}, required = true, description = "Path to the configuration file.")
    private String configfile;
    @Parameter(names = {"--filters", "-f"}, required = true, description = "Path to the file containing the filter definitions.")
    private String filters;

    public void read(@NonNull String path, @NonNull DomainManager domainManager) throws Exception {
        File file = new File(path);
        if (!file.exists()) {
            throw new IOException(String.format("File not found. [path=%s]", file.getAbsolutePath()));
        }
        try (FileReader fr = new FileReader(file)) {   //reads the file
            try (BufferedReader br = new BufferedReader(fr)) {  //creates a buffering character input stream
                StringBuffer sb = new StringBuffer();    //constructs a string buffer with no characters
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (Strings.isNullOrEmpty(line)) continue;
                    if (line.startsWith("#")) continue;

                    String[] parts = line.split(";");
                    if (parts.length >= 4) {
                        String d = parts[0];
                        String e = parts[1];
                        String p = parts[2];
                        String r = parts[3];
                        String g = null;
                        if (parts.length > 4) {
                            g = parts[4];
                        }
                        if (!Strings.isNullOrEmpty(d) &&
                                !Strings.isNullOrEmpty(p) &&
                                !Strings.isNullOrEmpty(r)) {
                            domainManager.add(d, e, p, r, g);
                            DefaultLogger.LOGGER.info(String.format("Registered Filter: [DOMAIN=%s][PATH=%s][REGEX=%s]", d, p, r));
                        }
                    }
                }
            }
        }
    }

    public static void main(String[] args) {
        try {
            String name = "DOMAIN_FILTER_LOADER";
            DomainFilterLoader loader = new DomainFilterLoader();
            JCommander.newBuilder().addObject(loader).build().parse(args);
            XMLConfiguration config = ConfigReader.read(loader.configfile, EConfigFileType.File);
            NameNodeEnv.setup(name, DomainFilterLoader.class, config);
            if (!(NameNodeEnv.get(name).stateManager() instanceof ProcessorStateManager)) {
                throw new Exception(
                        String.format("Invalid StateManager instance. [expected=%s]",
                                ProcessorStateManager.class.getCanonicalName()));
            }
            loader.read(loader.filters, ((ProcessorStateManager) NameNodeEnv.get(name).stateManager()).domainManager());
        } catch (Throwable t) {
            DefaultLogger.LOGGER.debug(DefaultLogger.stacktrace(t));
            DefaultLogger.LOGGER.error(t.getLocalizedMessage());
            t.printStackTrace();
        }
    }
}
