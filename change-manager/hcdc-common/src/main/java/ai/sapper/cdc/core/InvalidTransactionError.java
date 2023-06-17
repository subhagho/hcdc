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

package ai.sapper.cdc.core;

import ai.sapper.hcdc.common.model.DFSError;
import ai.sapper.hcdc.common.model.DFSFile;
import lombok.Getter;

import java.security.PrivilegedActionException;

@Getter
public class InvalidTransactionError extends Exception {
    private static final String __PREFIX = "Invalid DFS Transaction: %s";
    private final String hdfsPath;
    private final DFSError.ErrorCode errorCode;
    private DFSFile file;
    private long txId;

    public InvalidTransactionError withFile(DFSFile file) {
        this.file = file;
        return this;
    }

    /**
     * Constructs a new exception with the specified detail message and
     * cause.  <p>Note that the detail message associated with
     * {@code cause} is <i>not</i> automatically incorporated in
     * this exception's detail message.
     *
     * @param message the detail message (which is saved for later retrieval
     *                by the {@link #getMessage()} method).
     * @param cause   the cause (which is saved for later retrieval by the
     *                {@link #getCause()} method).  (A {@code null} value is
     *                permitted, and indicates that the cause is nonexistent or
     *                unknown.)
     * @since 1.4
     */
    public InvalidTransactionError(long txId, DFSError.ErrorCode errorCode, String hdfsPath, String message, Throwable cause) {
        super(String.format(__PREFIX, message), cause);
        this.hdfsPath = hdfsPath;
        this.errorCode = errorCode;
        this.txId = txId;
    }

    /**
     * Constructs a new exception with the specified cause and a detail
     * message of {@code (cause==null ? null : cause.toString())} (which
     * typically contains the class and detail message of {@code cause}).
     * This constructor is useful for exceptions that are little more than
     * wrappers for other throwables (for example, {@link
     * PrivilegedActionException}).
     *
     * @param cause the cause (which is saved for later retrieval by the
     *              {@link #getCause()} method).  (A {@code null} value is
     *              permitted, and indicates that the cause is nonexistent or
     *              unknown.)
     * @since 1.4
     */
    public InvalidTransactionError(long txId, DFSError.ErrorCode errorCode, String hdfsPath, Throwable cause) {
        super(String.format(__PREFIX, cause.getLocalizedMessage()), cause);
        this.hdfsPath = hdfsPath;
        this.errorCode = errorCode;
        this.txId = txId;
    }

    @Override
    public String getMessage() {
        return String.format("[TX=%d][path=%s][code=%s] %s", txId, hdfsPath, errorCode.name(), super.getMessage());
    }

    @Override
    public String getLocalizedMessage() {
        return String.format("[TX=%d][path=%s][code=%s] %s", txId, hdfsPath, errorCode.name(), super.getLocalizedMessage());
    }
}
