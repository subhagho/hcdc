package ai.sapper.hcdc.agents.namenode;

import ai.sapper.hcdc.agents.common.DFSEditsFileFinder;
import ai.sapper.hcdc.common.utils.DefaultLogger;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DFSEditsFileFinderTest {
    private static final String SOURCE_DIR = "src/test/resources/edits";

    @Test
    void findEditsFiles() {
        try {
            List<DFSEditsFileFinder.EditsLogFile> paths = DFSEditsFileFinder.findEditsFiles(SOURCE_DIR, 7, 17);
            assertNotNull(paths);
            for (DFSEditsFileFinder.EditsLogFile path : paths) {
                DefaultLogger.LOG.info(String.format("Files in Range [%s]", path));
            }
            paths = DFSEditsFileFinder.findEditsFiles(SOURCE_DIR, -1, 17);
            assertNotNull(paths);
            for (DFSEditsFileFinder.EditsLogFile path : paths) {
                DefaultLogger.LOG.info(String.format("File till TX [%s]", path));
            }
            paths = DFSEditsFileFinder.findEditsFiles(SOURCE_DIR, 17, -1);
            assertNotNull(paths);
            for (DFSEditsFileFinder.EditsLogFile path : paths) {
                DefaultLogger.LOG.info(String.format("Files with Start TX [%s]", path));
            }
        } catch (Throwable t) {
            DefaultLogger.LOG.debug(DefaultLogger.stacktrace(t));
            fail(t);
        }
    }

    @Test
    void findSeenTxID() {
        try {
            long txid = DFSEditsFileFinder.findSeenTxID(SOURCE_DIR);
            assertTrue(txid >= 0);
            DefaultLogger.LOG.info(String.format("Found TXID = %d", txid));
        } catch (Throwable t) {
            DefaultLogger.LOG.debug(DefaultLogger.stacktrace(t));
            fail(t);
        }
    }
}