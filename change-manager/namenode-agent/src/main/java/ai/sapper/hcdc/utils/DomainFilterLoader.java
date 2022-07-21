package ai.sapper.hcdc.utils;

import ai.sapper.hcdc.common.utils.DefaultLogger;
import ai.sapper.hcdc.core.filters.DomainManager;
import lombok.NonNull;
import org.apache.parquet.Strings;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

public class DomainFilterLoader {
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
                    if (parts.length == 3) {
                        String d = parts[0];
                        String p = parts[1];
                        String r = parts[2];
                        if (!Strings.isNullOrEmpty(d) &&
                                !Strings.isNullOrEmpty(p) &&
                                !Strings.isNullOrEmpty(r)) {
                            domainManager.add(d, p, r);
                            DefaultLogger.LOG.info(String.format("Registered Filter: [DOMAIN=%s][PATH=%s][REGEX=%s]", d, p, r));
                        }
                    }
                }
            }
        }
    }
}
