package ai.sapper.cdc.common.utils;

import lombok.NonNull;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.fail;

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
            DefaultLogger.LOGGER.info(String.format("Using temp file [path=%s]", fname));

            File file = new File(fname);
            if (file.exists()) {
                file.delete();
            }

            FileWatcher watcher = FileWatcherFactory.create("TEST", d.getAbsolutePath(), ".+\\.dat", new TestFileWatcherCallback());
            Runner runner = new Runner(file.getAbsolutePath());
            Thread tr = new Thread(runner);
            tr.start();
            tr.join();

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
                DefaultLogger.LOGGER.info(String.format("[%d] [%s] [%s]", count++, eventKind.name(), fp.toString()));
            } else {
                DefaultLogger.LOGGER.info(String.format("[%d] [%s] [%s] [size=%d]", count++, eventKind.name(), fp.toString(), Files.size(fp)));
            }
        }
    }

    private static class Runner implements Runnable {
        private final String path;
        private final String mesg = "Some options are file-level options, meaning they should be written at the top-level scope, not inside any message, enum, or service definition. Some options are message-level options, meaning they should be written inside message definitions. Some options are field-level options, meaning they should be written inside field definitions. Options can also be written on enum types, enum values, oneof fields, service types, and service methods; however, no useful options currently exist for any of these.";

        public Runner(@NonNull String path) {
            this.path = path;
        }

        /**
         * When an object implementing interface <code>Runnable</code> is used
         * to create a thread, starting the thread causes the object's
         * <code>run</code> method to be called in that separately executing
         * thread.
         * <p>
         * The general contract of the method <code>run</code> is that it may
         * take any action whatsoever.
         *
         * @see Thread#run()
         */
        @Override
        public void run() {
            try {
                File file = new File(path);

                Thread.sleep(5000);
                try (FileOutputStream fos = new FileOutputStream(file)) {
                    for (int ii = 0; ii < 10; ii++) {
                        String msg = String.format("[LINE=%d] %s\n", ii, mesg);
                        fos.write(msg.getBytes(StandardCharsets.UTF_8));
                        DefaultLogger.LOGGER.info(String.format("[LINE %d] written...", ii));
                        Thread.sleep(1000);
                    }
                }
                file.delete();
            } catch (Throwable t) {
                DefaultLogger.LOGGER.error(DefaultLogger.stacktrace(t));
            }
        }
    }
}