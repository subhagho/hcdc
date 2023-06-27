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
import ai.sapper.cdc.core.processing.MessageProcessorState;
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
public class HCdcMessageProcessingState<M extends ReceiverOffset> extends MessageProcessorState<EHCdcProcessorState, HCdcTxId, M> {
    private String lastMessageId;
    private SnapshotOffset snapshotOffset;
    private HCdcTxId receivedTx;

    public HCdcMessageProcessingState() {
        super(EHCdcProcessorState.Error,
                EHCdcProcessorState.Unknown,
                HCdcMessageProcessingState.class.getSimpleName());
    }

    public HCdcMessageProcessingState(@NonNull String type) {
        super(EHCdcProcessorState.Error,
                EHCdcProcessorState.Unknown,
                type);
    }

    public HCdcMessageProcessingState(@NonNull MessageProcessorState<EHCdcProcessorState, HCdcTxId, M> state) {
        super(state);
    }


    public boolean isLastProcessedMessage(@NonNull String messageId) {
        if (!Strings.isNullOrEmpty(lastMessageId)) {
            return lastMessageId.compareTo(messageId) == 0;
        }
        return false;
    }

    public HCdcMessageProcessingState<M> updateProcessedTxId(long processedTxId) throws Exception {
        if (getOffset().getId() > processedTxId) {
            throw new Exception(
                    String.format("Invalid transaction: [current=%d][specified=%s]",
                            getOffset().getId(), processedTxId));
        }
        getOffset().setId(processedTxId);
        return this;
    }

    public HCdcMessageProcessingState<M> updateProcessedRecordId(long processedTxId,
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

    public HCdcMessageProcessingState<M> updateReceivedTxId(long processedTxId) throws Exception {
        if (receivedTx == null) {
            receivedTx = new HCdcTxId(processedTxId);
        }
        if (receivedTx.getId() > processedTxId) {
            throw new Exception(
                    String.format("Invalid transaction: [current=%d][specified=%s]",
                            getOffset().getId(), processedTxId));
        }
        receivedTx.setId(processedTxId);
        return this;
    }

    public HCdcMessageProcessingState<M> updateReceivedRecordId(long processedTxId,
                                                                long recordId) throws Exception {
        if (receivedTx.getId() != processedTxId) {
            throw new Exception(
                    String.format("Invalid transaction: [current=%d][specified=%s]",
                            getOffset().getId(), processedTxId));
        }
        if (receivedTx.getRecordId() > recordId) {
            throw new Exception(
                    String.format("Invalid record: [current=%d][specified=%s]",
                            getOffset().getRecordId(), recordId));
        }
        receivedTx.setRecordId(recordId);
        return this;
    }

    public HCdcMessageProcessingState<M> updateSnapshotTxId(long snapshotTxId) throws Exception {
        Preconditions.checkNotNull(snapshotOffset);
        if (snapshotOffset.getSnapshotTxId() > snapshotTxId) {
            throw new Exception(
                    String.format("Invalid Snapshot transaction: [current=%d][specified=%s]",
                            snapshotOffset.getSnapshotTxId(), snapshotTxId));
        }
        snapshotOffset.setSnapshotTxId(snapshotTxId);
        return this;
    }

    public HCdcMessageProcessingState<M> updateSnapshotSequence(long snapshotTxId,
                                                                long sequence) throws Exception {
        Preconditions.checkNotNull(snapshotOffset);
        if (snapshotOffset.getSnapshotTxId() != snapshotTxId) {
            throw new Exception(
                    String.format("Invalid Snapshot transaction: [current=%d][specified=%s]",
                            snapshotOffset.getSnapshotTxId(), snapshotTxId));
        }
        if (snapshotOffset.getSnapshotSeq() < sequence) {
            throw new Exception(
                    String.format("Invalid Snapshot sequence: [current=%d][specified=%s]",
                            snapshotOffset.getSnapshotSeq(), sequence));
        }
        snapshotOffset.setSnapshotSeq(sequence);
        return this;
    }
}
