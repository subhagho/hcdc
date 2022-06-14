package ai.sapper.hcdc.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

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
                "src/test/resources/data"};
        HadoopDataLoader.main(args);
    }
}