package ai.sapper.hcdc.common.utils;

import lombok.NonNull;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class FileWatcherFactoryTest {

    @Test
    void create() {
        try {
            String dir = String.format("%s/test/", System.getProperty("java.io.tmpdir"));
            File d = new File(dir);
            if (!d.exists()) {
                d.mkdirs();
            }
            String fname = String.format("%s/%s.dat", d.getAbsolutePath(), UUID.randomUUID().toString());
            DefaultLogger.__LOG.info(String.format("Using temp file [path=%s]", fname));

            File file = new File(fname);
            if (file.exists()) {
                file.delete();
            }

            FileWatcher watcher = FileWatcherFactory.create("TEST", d.getAbsolutePath(), ".+\\.dat", new TestFileWatcherCallback());
            Thread.sleep(5000);
            try (FileOutputStream fos = new FileOutputStream(file)) {
                for (int ii = 0; ii < 10; ii++) {
                    String msg = String.format("[LINE=%d]\n", ii);
                    fos.write(msg.getBytes(StandardCharsets.UTF_8));
                    Thread.sleep(1000);
                }
            }
            file.delete();
            Thread.sleep(5000);
            FileWatcherFactory.shutdown();
        } catch (Throwable t) {
            fail(t);
        }
    }

    private static class TestFileWatcherCallback implements FileWatcher.FileWatcherCallback {
        private int count = 0;

        /**
         * @param path
         * @throws IOException
         */
        @Override
        public void handle(@NonNull String path, WatchEvent.Kind<?> eventKind) throws IOException {
            Path fp = Paths.get(path);
            if (eventKind.equals(StandardWatchEventKinds.ENTRY_DELETE)) {
                DefaultLogger.__LOG.info(String.format("[%d] [%s] [%s]", count++, eventKind.name(), fp.toString()));
            } else {
                DefaultLogger.__LOG.info(String.format("[%d] [%s] [%s] [size=%d]", count++, eventKind.name(), fp.toString(), Files.size(fp)));
            }
        }
    }
}