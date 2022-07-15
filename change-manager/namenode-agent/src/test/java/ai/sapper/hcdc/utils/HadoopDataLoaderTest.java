package ai.sapper.hcdc.utils;

import org.junit.jupiter.api.Test;

class HadoopDataLoaderTest {

    @Test
    void main() {
        System.setProperty("hadoop.home.dir", "C:/tools/hadoop");

        String[] args = {"--config",
                "src/test/resources/hdfs-loader-test.xml",
                "--input",
                "csv",
                "--output",
                "parquet",
                "--data",
                "src/test/resources/data",
                "--tmp",
                "/Work/temp/output/test",
                "--batchSize",
                "-1"};
        HadoopDataLoader.main(args);
    }
}