package ai.sapper.hcdc.utils;

import org.junit.jupiter.api.Test;

class HadoopDataLoaderTest {

    @Test
    void main() {
        System.setProperty("hadoop.home.dir", "/opt/hadoop/hadoop");
        for(int ii=0; ii < 10; ii++) {
            String[] args = {"--config",
                    "src/test/resources/hdfs-loader-test.xml",
                    "--input",
                    "csv",
                    "--output",
                    "avro",
                    "--data",
                    "src/test/resources/data",
                    "--tmp",
                    "/tmp/output/test",
                    "--batchSize",
                    "-1"};
            HadoopDataLoader.main(args);
        }
    }
}