package ai.sapper.cdc.common.utils;

import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

class PathUtilsTest {
    private static final String LOCAL_PATH = "src/test/java/ai/sapper/cdc/common/utils/FileWatcherFactoryTest.java";
    private static final String REMOTE_PATH = "https://raw.githubusercontent.com/subhagho/hcdc/1.2-SNAPSHOT/change-manager/namenode-agent/src/test/resources/configs/setup.xml";
    private static final String LOCAL_FILE_PATH = "file://src/test/java/ai/sapper/cdc/common/utils/PathUtilsTest.java";

    @Test
    void readFile() {
        try {
            File file = PathUtils.readFile(LOCAL_PATH);
            assertNotNull(file);
            file = PathUtils.readFile(LOCAL_FILE_PATH);
            assertNotNull(file);
            file = PathUtils.readFile(REMOTE_PATH);
            assertNotNull(file);
        } catch (Throwable t) {
            t.printStackTrace();
            fail(t);
        }
    }
}