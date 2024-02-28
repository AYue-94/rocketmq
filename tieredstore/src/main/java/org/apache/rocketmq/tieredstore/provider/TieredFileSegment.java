/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.tieredstore.provider;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.exception.TieredStoreErrorCode;
import org.apache.rocketmq.tieredstore.exception.TieredStoreException;
import org.apache.rocketmq.tieredstore.file.TieredCommitLog;
import org.apache.rocketmq.tieredstore.file.TieredConsumeQueue;
import org.apache.rocketmq.tieredstore.provider.stream.FileSegmentInputStream;
import org.apache.rocketmq.tieredstore.provider.stream.FileSegmentInputStreamFactory;
import org.apache.rocketmq.tieredstore.util.MessageBufferUtil;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;

import static org.apache.rocketmq.tieredstore.index.IndexStoreFile.INDEX_BEGIN_TIME_STAMP;
import static org.apache.rocketmq.tieredstore.index.IndexStoreFile.INDEX_END_TIME_STAMP;

public abstract class TieredFileSegment implements Comparable<TieredFileSegment>, TieredStoreProvider {

    private static final Logger logger = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);

    // 文件路径 broker/topic/queue
    protected final String filePath;
    // 当前segement的起始offset
    protected final long baseOffset;
    // commitlog/consumequeue/index
    protected final FileSegmentType fileType;
    protected final TieredMessageStoreConfig storeConfig;
    // segement最大容量
    private final long maxSize;
    private final ReentrantLock bufferLock = new ReentrantLock();
    private final Semaphore commitLock = new Semaphore(1);
    // segement是否写满
    private volatile boolean full = false;
    private volatile boolean closed = false;

    // segement中最小的存储时间
    private volatile long minTimestamp = Long.MAX_VALUE;
    // segement中最大的存储时间
    private volatile long maxTimestamp = Long.MAX_VALUE;
    // 刷盘进度
    private volatile long commitPosition = 0L;
    // append进度
    private volatile long appendPosition = 0L;

    // only used in commitLog
    // commitlog写入物理offset对应逻辑offset
    private volatile long dispatchCommitOffset = 0L;
    // 用于commitlog，标识一个segement的结束
    private ByteBuffer codaBuffer;
    // 待刷盘buffer
    private List<ByteBuffer> bufferList = new ArrayList<>();
    private FileSegmentInputStream fileSegmentInputStream;
    // 刷盘future
    private CompletableFuture<Boolean> flightCommitRequest = CompletableFuture.completedFuture(false);

    public TieredFileSegment(TieredMessageStoreConfig storeConfig,
        FileSegmentType fileType, String filePath, long baseOffset) {

        this.storeConfig = storeConfig;
        this.fileType = fileType;
        this.filePath = filePath;
        this.baseOffset = baseOffset;
        this.maxSize = getMaxSizeByFileType();
    }

    /**
     * The max segment size of a file is determined by the file type
     */
    protected long getMaxSizeByFileType() {
        switch (fileType) {
            case COMMIT_LOG:
                return storeConfig.getTieredStoreCommitLogMaxSize();
            case CONSUME_QUEUE:
                return storeConfig.getTieredStoreConsumeQueueMaxSize();
            case INDEX:
                return Long.MAX_VALUE;
            default:
                throw new IllegalArgumentException("Unsupported file type: " + fileType);
        }
    }

    @Override
    public int compareTo(TieredFileSegment o) {
        return Long.compare(this.baseOffset, o.baseOffset);
    }

    public long getBaseOffset() {
        return baseOffset;
    }

    public long getCommitOffset() {
        return baseOffset + commitPosition;
    }

    public long getCommitPosition() {
        return commitPosition;
    }

    public long getDispatchCommitOffset() {
        return dispatchCommitOffset;
    }

    public long getMaxOffset() {
        return baseOffset + appendPosition;
    }

    public long getMaxSize() {
        return maxSize;
    }

    public long getMinTimestamp() {
        return minTimestamp;
    }

    public void setMinTimestamp(long minTimestamp) {
        this.minTimestamp = minTimestamp;
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    public void setMaxTimestamp(long maxTimestamp) {
        this.maxTimestamp = maxTimestamp;
    }

    public boolean isFull() {
        return full;
    }

    public void setFull() {
        setFull(true);
    }

    public void setFull(boolean appendCoda) {
        bufferLock.lock();
        try {
            full = true;
            if (fileType == FileSegmentType.COMMIT_LOG && appendCoda) {
                appendCoda();
            }
        } finally {
            bufferLock.unlock();
        }
    }

    public boolean isClosed() {
        return closed;
    }

    public void close() {
        closed = true;
    }

    public FileSegmentType getFileType() {
        return fileType;
    }

    public void initPosition(long pos) {
        this.commitPosition = pos;
        this.appendPosition = pos;
    }

    private List<ByteBuffer> borrowBuffer() {
        bufferLock.lock();
        try {
            List<ByteBuffer> tmp = bufferList;
            bufferList = new ArrayList<>();
            return tmp;
        } finally {
            bufferLock.unlock();
        }
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    public AppendResult append(ByteBuffer byteBuf, long timestamp) {
        if (closed) {
            return AppendResult.FILE_CLOSED;
        }

        bufferLock.lock();
        try {
            if (full || codaBuffer != null) {
                return AppendResult.FILE_FULL;
            }

            if (fileType == FileSegmentType.INDEX) {
                minTimestamp = byteBuf.getLong(INDEX_BEGIN_TIME_STAMP);
                maxTimestamp = byteBuf.getLong(INDEX_END_TIME_STAMP);

                appendPosition += byteBuf.remaining();
                // IndexFile is large and not change after compaction, no need deep copy
                bufferList.add(byteBuf);
                setFull();
                return AppendResult.SUCCESS;
            }

            // 文件满了，设置full=true，返回FILE_FULL
            if (appendPosition + byteBuf.remaining() > maxSize) {
                setFull();
                return AppendResult.FILE_FULL;
            }

            // 到达批量刷盘阈值，异步刷盘
            if (bufferList.size() > storeConfig.getTieredStoreGroupCommitCount() // 2500
                || appendPosition - commitPosition > storeConfig.getTieredStoreGroupCommitSize()) { // 32MB
                commitAsync();
            }

            // 如果buffer长度超过1w，不可append
            if (bufferList.size() > storeConfig.getTieredStoreMaxGroupCommitCount()) {
                logger.debug("File segment append buffer full, file: {}, buffer size: {}, pending bytes: {}",
                    getPath(), bufferList.size(), appendPosition - commitPosition);
                return AppendResult.BUFFER_FULL;
            }

            // 存储时间戳更新
            if (timestamp != Long.MAX_VALUE) {
                maxTimestamp = timestamp;
                if (minTimestamp == Long.MAX_VALUE) {
                    minTimestamp = timestamp;
                }
            }

            // append进度更新
            appendPosition += byteBuf.remaining();

            // buffer复制，加入bufferList
            // deep copy buffer
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(byteBuf.remaining());
            byteBuffer.put(byteBuf);
            byteBuffer.flip();
            byteBuf.rewind();

            bufferList.add(byteBuffer);
            return AppendResult.SUCCESS;
        } finally {
            bufferLock.unlock();
        }
    }

    public void setCommitPosition(long commitPosition) {
        this.commitPosition = commitPosition;
    }

    public long getAppendPosition() {
        return appendPosition;
    }

    public void setAppendPosition(long appendPosition) {
        this.appendPosition = appendPosition;
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    private void appendCoda() {
        if (codaBuffer != null) {
            return;
        }
        codaBuffer = ByteBuffer.allocate(TieredCommitLog.CODA_SIZE);
        codaBuffer.putInt(TieredCommitLog.CODA_SIZE);
        codaBuffer.putInt(TieredCommitLog.BLANK_MAGIC_CODE);
        codaBuffer.putLong(maxTimestamp);
        codaBuffer.flip();
        appendPosition += TieredCommitLog.CODA_SIZE;
    }

    public ByteBuffer read(long position, int length) {
        return readAsync(position, length).join();
    }

    public CompletableFuture<ByteBuffer> readAsync(long position, int length) {
        CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
        if (position < 0 || length < 0) {
            future.completeExceptionally(
                new TieredStoreException(TieredStoreErrorCode.ILLEGAL_PARAM, "position or length is negative"));
            return future;
        }
        if (length == 0) {
            future.completeExceptionally(
                new TieredStoreException(TieredStoreErrorCode.ILLEGAL_PARAM, "length is zero"));
            return future;
        }
        if (position >= commitPosition) {
            future.completeExceptionally(
                new TieredStoreException(TieredStoreErrorCode.ILLEGAL_PARAM, "position is illegal"));
            return future;
        }
        if (position + length > commitPosition) {
            logger.debug("TieredFileSegment#readAsync request position + length is greater than commit position," +
                    " correct length using commit position, file: {}, request position: {}, commit position:{}, change length from {} to {}",
                getPath(), position, commitPosition, length, commitPosition - position);
            length = (int) (commitPosition - position);
            if (length == 0) {
                future.completeExceptionally(
                    new TieredStoreException(TieredStoreErrorCode.NO_NEW_DATA, "request position is equal to commit position"));
                return future;
            }
            if (fileType == FileSegmentType.CONSUME_QUEUE && length % TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE != 0) {
                future.completeExceptionally(
                    new TieredStoreException(TieredStoreErrorCode.ILLEGAL_PARAM, "position and length is illegal"));
                return future;
            }
        }
        return read0(position, length);
    }

    public boolean needCommit() {
        return appendPosition > commitPosition;
    }

    public boolean commit() {
        if (closed) {
            return false;
        }
        // result is false when we send real commit request
        // use join for wait flight request done
        Boolean result = commitAsync().join();
        if (!result) {
            result = flightCommitRequest.join();
        }
        return result;
    }

    private void releaseCommitLock() {
        if (commitLock.availablePermits() == 0) {
            commitLock.release();
        } else {
            logger.error("[Bug] FileSegmentCommitAsync, lock is already released: available permits: {}",
                commitLock.availablePermits());
        }
    }

    private void updateDispatchCommitOffset(List<ByteBuffer> bufferList) {
        if (fileType == FileSegmentType.COMMIT_LOG && bufferList.size() > 0) {
            dispatchCommitOffset =
                MessageBufferUtil.getQueueOffset(bufferList.get(bufferList.size() - 1));
        }
    }

    /**
     * @return false: commit, true: no commit operation
     */
    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    public CompletableFuture<Boolean> commitAsync() {
        if (closed) {
            return CompletableFuture.completedFuture(false);
        }

        if (!needCommit()) {
            return CompletableFuture.completedFuture(true);
        }

        if (commitLock.drainPermits() <= 0) {
            return CompletableFuture.completedFuture(false);
        }

        try {
            // 异常情况处理
            if (fileSegmentInputStream != null) {
                long fileSize = this.getSize();
                if (fileSize == -1L) {
                    logger.error("Get commit position error before commit, Commit: {}, Expect: {}, Current Max: {}, FileName: {}",
                        commitPosition, commitPosition + fileSegmentInputStream.getContentLength(), appendPosition, getPath());
                    releaseCommitLock();
                    return CompletableFuture.completedFuture(false);
                } else {
                    if (correctPosition(fileSize, null)) {
                        updateDispatchCommitOffset(fileSegmentInputStream.getBufferList());
                        fileSegmentInputStream = null;
                    }
                }
            }

            int bufferSize;
            if (fileSegmentInputStream != null) {
                // 异常情况处理
                bufferSize = fileSegmentInputStream.available();
            } else {
                // 获取bufferList
                List<ByteBuffer> bufferList = borrowBuffer();
                bufferSize = bufferList.stream().mapToInt(ByteBuffer::remaining).sum()
                    + (codaBuffer != null ? codaBuffer.remaining() : 0);
                if (bufferSize == 0) {
                    releaseCommitLock();
                    return CompletableFuture.completedFuture(true);
                }
                // 屏蔽bufferList，构造InputStream
                fileSegmentInputStream = FileSegmentInputStreamFactory.build(
                    fileType, baseOffset + commitPosition, bufferList, codaBuffer, bufferSize);
            }

            return flightCommitRequest = this
                // 调用用户实现commit0
                .commit0(fileSegmentInputStream, commitPosition, bufferSize, fileType != FileSegmentType.INDEX)
                .thenApply(result -> {
                    if (result) {
                        // 如果是commitlog，更新dispatchCommitOffset
                        updateDispatchCommitOffset(fileSegmentInputStream.getBufferList());
                        // 更新commit位点
                        commitPosition += bufferSize;
                        fileSegmentInputStream = null;
                        return true;
                    } else {
                        // 所有position回滚到0
                        fileSegmentInputStream.rewind();
                        return false;
                    }
                })
                // 异常处理
                .exceptionally(this::handleCommitException)
                .whenComplete((result, e) -> releaseCommitLock());

        } catch (Exception e) {
            handleCommitException(e);
            releaseCommitLock();
        }
        return CompletableFuture.completedFuture(false);
    }

    private long getCorrectFileSize(Throwable throwable) {
        if (throwable instanceof TieredStoreException) {
            long fileSize = ((TieredStoreException) throwable).getPosition();
            if (fileSize > 0) {
                return fileSize;
            }
        }
        return getSize();
    }

    private boolean handleCommitException(Throwable e) {
        // Get root cause here
        Throwable cause = e.getCause() != null ? e.getCause() : e;

        // 1. 调用segment用户实现getFileSize，获取实际文件长度
        long fileSize = this.getCorrectFileSize(cause);

        // 2. 如果返回-1，回滚InputStream
        if (fileSize == -1L) {
            logger.error("Get commit position error, Commit: %d, Expect: %d, Current Max: %d, FileName: %s",
                commitPosition, commitPosition + fileSegmentInputStream.getContentLength(), appendPosition, getPath());
            fileSegmentInputStream.rewind();
            return false;
        }

        // 3. 根据文件大小，修复刷盘进度，决定是否回滚InputStream
        if (correctPosition(fileSize, cause)) {
            updateDispatchCommitOffset(fileSegmentInputStream.getBufferList());
            fileSegmentInputStream = null;
            return true;
        } else {
            fileSegmentInputStream.rewind();
            return false;
        }
    }

    /**
     * return true to clear buffer
     */
    private boolean correctPosition(long fileSize, Throwable throwable) {

        // Current we have three offsets here: commit offset, expect offset, file size.
        // We guarantee that the commit offset is less than or equal to the expect offset.
        // Max offset will increase because we can continuously put in new buffers
        String handleInfo = throwable == null ? "before commit" : "after commit";
        long expectPosition = commitPosition + fileSegmentInputStream.getContentLength();

        String offsetInfo = String.format("Correct Commit Position, %s, result=[{}], " +
                "Commit: %d, Expect: %d, Current Max: %d, FileSize: %d, FileName: %s",
            handleInfo, commitPosition, expectPosition, appendPosition, fileSize, this.getPath());

        // We are believing that the file size returned by the server is correct,
        // can reset the commit offset to the file size reported by the storage system.
        if (fileSize == expectPosition) {
            logger.info(offsetInfo, "Success", throwable);
            commitPosition = fileSize;
            return true;
        }

        if (fileSize < commitPosition) {
            logger.error(offsetInfo, "FileSizeIncorrect", throwable);
        } else if (fileSize == commitPosition) {
            logger.warn(offsetInfo, "CommitFailed", throwable);
        } else if (fileSize > commitPosition) {
            logger.warn(offsetInfo, "PartialSuccess", throwable);
        }
        commitPosition = fileSize;
        return false;
    }
}
