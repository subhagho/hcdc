package ai.sapper.hcdc.agents.namenode;

import ai.sapper.hcdc.common.utils.DefaultLogger;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

class DFSEditsFileFinderTest {
    private static final String SOURCE_DIR = "src/test/resources/edits";

    @Test
    void findEditsFiles() {
        try {
            List<String> paths = DFSEditsFileFinder.findEditsFiles(SOURCE_DIR, 7, 17);
            assertNotNull(paths);
            for (String path : paths) {
                DefaultLogger.LOG.info(String.format("Files in Range [%s]", path));
            }
            paths = DFSEditsFileFinder.findEditsFiles(SOURCE_DIR, -1, 17);
            assertNotNull(paths);
            for (String path : paths) {
                DefaultLogger.LOG.info(String.format("File till TX [%s]", path));
            }
            paths = DFSEditsFileFinder.findEditsFiles(SOURCE_DIR, 17, -1);
            assertNotNull(paths);
            for (String path : paths) {
                DefaultLogger.LOG.info(String.format("Files with Start TX [%s]", path));
            }
        } catch (Throwable t) {
            DefaultLogger.LOG.debug(DefaultLogger.stacktrace(t));
            fail(t);
        }
    }
}