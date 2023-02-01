package ai.sapper.cdc.core.io.impl.s3;

import ai.sapper.cdc.core.io.PathInfo;
import ai.sapper.cdc.core.io.SyncFramework;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

public class S3SyncFramework extends SyncFramework {
    private final S3FileSystem fs;

    public S3SyncFramework(@NonNull S3FileSystem fs) {
        this.fs = fs;
    }

    @Override
    public void download(@NonNull PathInfo path) throws Exception {

    }

    @Override
    public void upload(@NonNull PathInfo path) throws Exception {

    }

    @Getter
    @Setter
    public static class S3SyncResponse {
        private long queueTimestamp;
        private long startTimestamp;
        private PathInfo path;
        private long finishTimestamp;
    }

    public static class S3DownloadTask extends Task<S3SyncResponse> {
        private final S3FileSystem fs;
        private final S3SyncResponse response = new S3SyncResponse();

        public S3DownloadTask(@NonNull PathInfo path, @NonNull S3FileSystem fs) {
            super(path);
            this.fs = fs;
            response.queueTimestamp = System.currentTimeMillis();
        }

        @Override
        public S3SyncResponse execute() throws Exception {
            try {
                response.startTimestamp = System.currentTimeMillis();
                fs.download((S3PathInfo) path());
                return response;
            } finally {
                response.finishTimestamp = System.currentTimeMillis();
            }
        }
    }
}
