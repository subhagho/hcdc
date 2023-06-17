/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.sapper.cdc.core.model;

import ai.sapper.cdc.core.messaging.ReceiverOffset;
import ai.sapper.cdc.core.processing.ProcessingState;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(
        use = JsonTypeInfo.Id.CLASS,
        include = JsonTypeInfo.As.PROPERTY,
        property = "@class"
)
public class HCdcProcessingState extends ProcessingState<EHCdcProcessorState, HCdcTxId> {
    private String lastMessageId;
    private SnapshotOffset snapshotOffset;

    public HCdcProcessingState() {
        super(EHCdcProcessorState.Error, EHCdcProcessorState.Unknown);
        snapshotOffset = new SnapshotOffset();
    }

    public HCdcProcessingState(@NonNull ReceiverOffset messageOffset) {
        super(EHCdcProcessorState.Error, EHCdcProcessorState.Unknown);
        setOffset(new HCdcTxId());
    }

    public HCdcProcessingState(@NonNull HCdcProcessingState state) {
        super(state);
        snapshotOffset = state.getSnapshotOffset();
        lastMessageId = state.getLastMessageId();
    }

    public boolean isLastProcessedMessage(@NonNull String messageId) {
        if (!Strings.isNullOrEmpty(lastMessageId)) {
            return lastMessageId.compareTo(messageId) == 0;
        }
        return false;
    }

    public HCdcProcessingState updateProcessedTxId(long processedTxId) throws Exception {
        if (getOffset().getId() > processedTxId) {
            throw new Exception(
                    String.format("Invalid transaction: [current=%d][specified=%s]",
                            getOffset().getId(), processedTxId));
        }
        getOffset().setId(processedTxId);
        return this;
    }

    public HCdcProcessingState updateProcessedRecordId(long processedTxId,
                                                       long recordId) throws Exception {
        if (getOffset().getId() != processedTxId) {
            throw new Exception(
                    String.format("Invalid transaction: [current=%d][specified=%s]",
                            getOffset().getId(), processedTxId));
        }
        if (getOffset().getRecordId() > recordId) {
            throw new Exception(
                    String.format("Invalid record: [current=%d][specified=%s]",
                            getOffset().getRecordId(), recordId));
        }
        getOffset().setRecordId(recordId);
        return this;
    }

    public HCdcProcessingState updateSnapshotTxId(long snapshotTxId) throws Exception {
        Preconditions.checkNotNull(snapshotOffset);
        if (snapshotOffset.getSnapshotTxId() > snapshotTxId) {
            throw new Exception(
                    String.format("Invalid Snapshot transaction: [current=%d][specified=%s]",
                            snapshotOffset.getSnapshotTxId(), snapshotTxId));
        }
        snapshotOffset.setSnapshotTxId(snapshotTxId);
        snapshotOffset.setUpdatedTimestamp(System.currentTimeMillis());
        return this;
    }

    public HCdcProcessingState updateSnapshotSequence(long snapshotTxId,
                                                      long sequence) throws Exception {
        Preconditions.checkNotNull(snapshotOffset);
        if (snapshotOffset.getSnapshotTxId() != snapshotTxId) {
            throw new Exception(
                    String.format("Invalid Snapshot transaction: [current=%d][specified=%s]",
                            snapshotOffset.getSnapshotTxId(), snapshotTxId));
        }
        if (snapshotOffset.getSnapshotSeq()  >= 0
                && snapshotOffset.getSnapshotSeq() < sequence) {
            throw new Exception(
                    String.format("Invalid Snapshot sequence: [current=%d][specified=%s]",
                            snapshotOffset.getSnapshotSeq(), sequence));
        }
        snapshotOffset.setSnapshotSeq(sequence);
        snapshotOffset.setUpdatedTimestamp(System.currentTimeMillis());
        return this;
    }
}
