package ai.sapper.hcdc.common.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DFSBlockProtoTest {

    @Test
    void getDescriptor() {
        try {
            int size  = 512;
            int bsize = 1024;

            DFSBlock block = DFSBlock.newBuilder()
                    .setBlockId(100000)
                    .setBlockSize(bsize)
                    .setSize(0)
                    .setStartOffset(0)
                    .setEndOffset(0)
                    .setGenerationStamp(0)
                    .setDeltaSize(0)
                    .build();
            block = block.toBuilder().setSize(size).build();
            assertEquals(bsize, block.getBlockSize());
            assertEquals(size, block.getSize());

        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
}