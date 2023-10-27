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

package org.apache.rocketmq.store.ha.autoswitch;


import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.ConcurrentHashMapUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.EpochEntry;
import org.apache.rocketmq.remoting.protocol.body.HARuntimeInfo;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.ha.DefaultHAService;
import org.apache.rocketmq.store.ha.GroupTransferService;
import org.apache.rocketmq.store.ha.HAClient;
import org.apache.rocketmq.store.ha.HAConnection;
import org.apache.rocketmq.store.ha.HAConnectionStateNotificationService;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * SwitchAble ha service, support switch role to master or slave.
 */
public class AutoSwitchHAService extends DefaultHAService {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private final ExecutorService executorService = Executors.newSingleThreadExecutor(new ThreadFactoryImpl("AutoSwitchHAService_Executor_"));

    // slave副本-上次追上master的时间
    private final ConcurrentHashMap<Long/*brokerId*/, Long/*lastCaughtUpTimestamp*/> connectionCaughtUpTimeTable = new ConcurrentHashMap<>();

    // 通知controller syncStateSet变化
    private final List<Consumer<Set<Long/*brokerId*/>>> syncStateSetChangedListeners = new ArrayList<>();

    // syncStateSet
    private final Set<Long/*brokerId*/> syncStateSet = new HashSet<>();
    private final Set<Long> remoteSyncStateSet = new HashSet<>();
    //  Indicate whether the syncStateSet is currently in the process of being synchronized to controller.
    private volatile boolean isSynchronizingSyncStateSet = false;

    private final ReadWriteLock syncStateSetReadWriteLock = new ReentrantReadWriteLock();
    private final Lock readLock = syncStateSetReadWriteLock.readLock();
    private final Lock writeLock = syncStateSetReadWriteLock.writeLock();

    // 每个master任期内 同步commitlog的offset范围
    private EpochFileCache epochCache;
    // slave角色haClient
    private AutoSwitchHAClient haClient;

    // 当前broker在controller侧id，brokerControllerId
    private Long brokerControllerId = null;

    public AutoSwitchHAService() {
    }

    @Override
    public void init(final DefaultMessageStore defaultMessageStore) throws IOException {
        this.epochCache = new EpochFileCache(defaultMessageStore.getMessageStoreConfig().getStorePathEpochFile());
        this.epochCache.initCacheFromFile();
        this.defaultMessageStore = defaultMessageStore;
        this.acceptSocketService = new AutoSwitchAcceptSocketService(defaultMessageStore.getMessageStoreConfig());
        this.groupTransferService = new GroupTransferService(this, defaultMessageStore);
        this.haConnectionStateNotificationService = new HAConnectionStateNotificationService(this, defaultMessageStore);
    }

    @Override
    public void shutdown() {
        super.shutdown();
        if (this.haClient != null) {
            this.haClient.shutdown();
        }
        this.executorService.shutdown();
    }

    @Override
    public void removeConnection(HAConnection conn) {
        if (!defaultMessageStore.isShutdown()) {
            final Set<Long> syncStateSet = getLocalSyncStateSet();
            Long slave = ((AutoSwitchHAConnection) conn).getSlaveId();
            if (syncStateSet.contains(slave)) {
                syncStateSet.remove(slave);
                // 加入remoteSyncStateSet
                markSynchronizingSyncStateSet(syncStateSet);
                // 通知controller
                notifySyncStateSetChanged(syncStateSet);
            }
        }
        super.removeConnection(conn);
    }

    @Override
    public boolean changeToMaster(int masterEpoch) {
        final int lastEpoch = this.epochCache.lastEpoch();
        if (masterEpoch < lastEpoch) {
            LOGGER.warn("newMasterEpoch {} < lastEpoch {}, fail to change to master", masterEpoch, lastEpoch);
            return false;
        }
        // 1. 通讯层connection清理
        destroyConnections();
        // Stop ha client if needed
        if (this.haClient != null) {
            this.haClient.shutdown();
        }

        // Truncate dirty file
        // 2. 截断commitlog和consumequeue
        final long truncateOffset = truncateInvalidMsg();

        // 3. 计算confirmOffset，这里会取当前commitlog的最大offset
        this.defaultMessageStore.setConfirmOffset(computeConfirmOffset());

        // 4. 按照上面commitlog计算的offset，截断epoch文件中记录的offset
        if (truncateOffset >= 0) {
            this.epochCache.truncateSuffixByOffset(truncateOffset);
        }

        // Append new epoch to epochFile

        // 5. 创建新的epoch记录
        final EpochEntry newEpochEntry = new EpochEntry(masterEpoch, this.defaultMessageStore.getMaxPhyOffset());
        if (this.epochCache.lastEpoch() >= masterEpoch) {
            this.epochCache.truncateSuffixByEpoch(masterEpoch);
        }
        this.epochCache.appendEntry(newEpochEntry);

        // Waiting consume queue dispatch
        // 6. 等待reput（consumequeue）追上commitlog
        while (defaultMessageStore.dispatchBehindBytes() > 0) {
            try {
                Thread.sleep(100);
            } catch (Exception ignored) {

            }
        }

        // 7. 开启TransientStorePool，等待 writebuffer commit 到 commitlog文件
        if (defaultMessageStore.isTransientStorePoolEnable()) {
            waitingForAllCommit();
            defaultMessageStore.getTransientStorePool().setRealCommit(true);
        }

        LOGGER.info("TruncateOffset is {}, confirmOffset is {}, maxPhyOffset is {}", truncateOffset, this.defaultMessageStore.getConfirmOffset(), this.defaultMessageStore.getMaxPhyOffset());
        this.defaultMessageStore.recoverTopicQueueTable();
        this.defaultMessageStore.setStateMachineVersion(masterEpoch);
        LOGGER.info("Change ha to master success, newMasterEpoch:{}, startOffset:{}", masterEpoch, newEpochEntry.getStartOffset());
        return true;
    }

    @Override
    public boolean changeToSlave(String newMasterAddr, int newMasterEpoch, Long slaveId) {
        final int lastEpoch = this.epochCache.lastEpoch();
        if (newMasterEpoch < lastEpoch) {
            LOGGER.warn("newMasterEpoch {} < lastEpoch {}, fail to change to slave", newMasterEpoch, lastEpoch);
            return false;
        }
        try {
            // 1. 关闭master才会有的HAConnection
            destroyConnections();
            // 2. 开启slave才用的haClient
            if (this.haClient == null) {
                this.haClient = new AutoSwitchHAClient(this, defaultMessageStore, this.epochCache, slaveId);
            } else {
                this.haClient.reOpen();
            }
            this.haClient.updateMasterAddress(newMasterAddr);
            this.haClient.updateHaMasterAddress(null);
            this.haClient.start();

            // 3. 开启TransientStorePool，等待 writebuffer commit
            if (defaultMessageStore.isTransientStorePoolEnable()) {
                waitingForAllCommit();
                defaultMessageStore.getTransientStorePool().setRealCommit(false);
            }

            this.defaultMessageStore.setStateMachineVersion(newMasterEpoch);

            LOGGER.info("Change ha to slave success, newMasterAddress:{}, newMasterEpoch:{}", newMasterAddr, newMasterEpoch);
            return true;
        } catch (final Exception e) {
            LOGGER.error("Error happen when change ha to slave", e);
            return false;
        }
    }

    @Override
    public boolean changeToMasterWhenLastRoleIsMaster(int masterEpoch) {
        final int lastEpoch = this.epochCache.lastEpoch();
        if (masterEpoch < lastEpoch) {
            LOGGER.warn("newMasterEpoch {} < lastEpoch {}, fail to change to master", masterEpoch, lastEpoch);
            return false;
        }
        // Append new epoch to epochFile
        final EpochEntry newEpochEntry = new EpochEntry(masterEpoch, this.defaultMessageStore.getMaxPhyOffset());
        if (this.epochCache.lastEpoch() >= masterEpoch) {
            this.epochCache.truncateSuffixByEpoch(masterEpoch);
        }
        this.epochCache.appendEntry(newEpochEntry);

        this.defaultMessageStore.setStateMachineVersion(masterEpoch);
        LOGGER.info("Change ha to master success, last role is master, newMasterEpoch:{}, startOffset:{}",
            masterEpoch, newEpochEntry.getStartOffset());
        return true;
    }

    @Override
    public boolean changeToSlaveWhenMasterNotChange(String newMasterAddr, int newMasterEpoch) {
        final int lastEpoch = this.epochCache.lastEpoch();
        if (newMasterEpoch < lastEpoch) {
            LOGGER.warn("newMasterEpoch {} < lastEpoch {}, fail to change to slave", newMasterEpoch, lastEpoch);
            return false;
        }

        this.defaultMessageStore.setStateMachineVersion(newMasterEpoch);
        LOGGER.info("Change ha to slave success, master doesn't change, newMasterAddress:{}, newMasterEpoch:{}",
            newMasterAddr, newMasterEpoch);
        return true;
    }

    public void waitingForAllCommit() {
        while (getDefaultMessageStore().remainHowManyDataToCommit() > 0) {
            getDefaultMessageStore().getCommitLog().getFlushManager().wakeUpCommit();
            try {
                Thread.sleep(100);
            } catch (Exception e) {

            }
        }
    }

    @Override
    public HAClient getHAClient() {
        return this.haClient;
    }

    @Override
    public void updateHaMasterAddress(String newAddr) {
        if (this.haClient != null) {
            this.haClient.updateHaMasterAddress(newAddr);
        }
    }

    @Override
    public void updateMasterAddress(String newAddr) {
    }

    public void registerSyncStateSetChangedListener(final Consumer<Set<Long>> listener) {
        this.syncStateSetChangedListeners.add(listener);
    }

    public void notifySyncStateSetChanged(final Set<Long> newSyncStateSet) {
        this.executorService.submit(() -> {
            syncStateSetChangedListeners.forEach(listener -> listener.accept(newSyncStateSet));
        });
        LOGGER.info("Notify the syncStateSet has been changed into {}.", newSyncStateSet);
    }

    /**
     * Check and maybe shrink the SyncStateSet.
     * A slave will be removed from SyncStateSet if (curTime - HaConnection.lastCaughtUpTime) > option(haMaxTimeSlaveNotCatchup)
     */
    public Set<Long> maybeShrinkSyncStateSet() {
        final Set<Long> newSyncStateSet = getLocalSyncStateSet();
        boolean isSyncStateSetChanged = false;
        final long haMaxTimeSlaveNotCatchup = this.defaultMessageStore.getMessageStoreConfig().getHaMaxTimeSlaveNotCatchup();
        // 扫描connectionCaughtUpTimeTable
        for (Map.Entry<Long, Long> next : this.connectionCaughtUpTimeTable.entrySet()) {
            final Long slaveBrokerId = next.getKey();
            if (newSyncStateSet.contains(slaveBrokerId)) {
                final Long lastCaughtUpTimeMs = next.getValue();
                // 超过15s未追上master，从SyncStateSet移除
                if ((System.currentTimeMillis() - lastCaughtUpTimeMs) > haMaxTimeSlaveNotCatchup) {
                    newSyncStateSet.remove(slaveBrokerId);
                    isSyncStateSetChanged = true;
                }
            }
        }

        // If the slaveBrokerId is in syncStateSet but not in connectionCaughtUpTimeTable,
        // it means that the broker has not connected.
        // SyncStateSet中的slave，没有建立连接，所以不在connectionCaughtUpTimeTable
        for (Long slaveBrokerId : newSyncStateSet) {
            if (!this.connectionCaughtUpTimeTable.containsKey(slaveBrokerId)) {
                newSyncStateSet.remove(slaveBrokerId);
                isSyncStateSetChanged = true;
            }
        }

        if (isSyncStateSetChanged) {
            markSynchronizingSyncStateSet(newSyncStateSet);
        }
        return newSyncStateSet;
    }

    /**
     * Check and maybe add the slave to SyncStateSet. A slave will be added to SyncStateSet if its slaveMaxOffset >=
     * current confirmOffset, and it is caught up to an offset within the current leader epoch.
     */
    public void maybeExpandInSyncStateSet(final Long slaveBrokerId, final long slaveMaxOffset) {
        // 1. slave在SyncStateSet中，直接返回
        final Set<Long> currentSyncStateSet = getLocalSyncStateSet();
        if (currentSyncStateSet.contains(slaveBrokerId)) {
            return;
        }
        final long confirmOffset = this.defaultMessageStore.getConfirmOffset();
        // 2. slave的offset追上master的confirmOffset
        if (slaveMaxOffset >= confirmOffset) {
            final EpochEntry currentLeaderEpoch = this.epochCache.lastEntry();
            // 3. slave的offset在当前epoch中
            if (slaveMaxOffset >= currentLeaderEpoch.getStartOffset()) {
                LOGGER.info("The slave {} has caught up, slaveMaxOffset: {}, confirmOffset: {}, epoch: {}, leader epoch startOffset: {}.",
                        slaveBrokerId, slaveMaxOffset, confirmOffset, currentLeaderEpoch.getEpoch(), currentLeaderEpoch.getStartOffset());
                currentSyncStateSet.add(slaveBrokerId);
                // 加入remoteSyncStateSet
                markSynchronizingSyncStateSet(currentSyncStateSet);
                // Notify the upper layer that syncStateSet changed.
                // 通知controller SyncStateSet发生变化
                notifySyncStateSetChanged(currentSyncStateSet);
            }
        }
    }

    // SyncStateSet change phase1
    private void markSynchronizingSyncStateSet(final Set<Long> newSyncStateSet) {
        this.writeLock.lock();
        try {
            this.isSynchronizingSyncStateSet = true;
            this.remoteSyncStateSet.clear();
            this.remoteSyncStateSet.addAll(newSyncStateSet);
        } finally {
            this.writeLock.unlock();
        }
    }

    private void markSynchronizingSyncStateSetDone() {
        // No need to lock, because the upper-level calling method has already locked write lock
        this.isSynchronizingSyncStateSet = false;
    }

    public boolean isSynchronizingSyncStateSet() {
        return isSynchronizingSyncStateSet;
    }

    public void updateConnectionLastCaughtUpTime(final Long slaveBrokerId, final long lastCaughtUpTimeMs) {
        Long prevTime = ConcurrentHashMapUtils.computeIfAbsent(this.connectionCaughtUpTimeTable, slaveBrokerId, k -> 0L);
        this.connectionCaughtUpTimeTable.put(slaveBrokerId, Math.max(prevTime, lastCaughtUpTimeMs));
    }

    public void updateConfirmOffsetWhenSlaveAck(final Long slaveBrokerId) {
        this.readLock.lock();
        try {
            if (this.syncStateSet.contains(slaveBrokerId)) {
                this.defaultMessageStore.setConfirmOffset(computeConfirmOffset());
            }
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public int inSyncReplicasNums(final long masterPutWhere) {
        this.readLock.lock();
        try {
            if (this.isSynchronizingSyncStateSet) {
                return Math.max(this.syncStateSet.size(), this.remoteSyncStateSet.size());
            } else {
                return this.syncStateSet.size();
            }
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public HARuntimeInfo getRuntimeInfo(long masterPutWhere) {
        HARuntimeInfo info = new HARuntimeInfo();

        if (BrokerRole.SLAVE.equals(this.getDefaultMessageStore().getMessageStoreConfig().getBrokerRole())) {
            info.setMaster(false);

            info.getHaClientRuntimeInfo().setMasterAddr(this.haClient.getHaMasterAddress());
            info.getHaClientRuntimeInfo().setMaxOffset(this.getDefaultMessageStore().getMaxPhyOffset());
            info.getHaClientRuntimeInfo().setLastReadTimestamp(this.haClient.getLastReadTimestamp());
            info.getHaClientRuntimeInfo().setLastWriteTimestamp(this.haClient.getLastWriteTimestamp());
            info.getHaClientRuntimeInfo().setTransferredByteInSecond(this.haClient.getTransferredByteInSecond());
            info.getHaClientRuntimeInfo().setMasterFlushOffset(this.defaultMessageStore.getMasterFlushedOffset());
        } else {
            info.setMaster(true);

            info.setMasterCommitLogMaxOffset(masterPutWhere);

            Set<Long> localSyncStateSet = getLocalSyncStateSet();
            for (HAConnection conn : this.connectionList) {
                HARuntimeInfo.HAConnectionRuntimeInfo cInfo = new HARuntimeInfo.HAConnectionRuntimeInfo();

                long slaveAckOffset = conn.getSlaveAckOffset();
                cInfo.setSlaveAckOffset(slaveAckOffset);
                cInfo.setDiff(masterPutWhere - slaveAckOffset);
                cInfo.setAddr(conn.getClientAddress().substring(1));
                cInfo.setTransferredByteInSecond(conn.getTransferredByteInSecond());
                cInfo.setTransferFromWhere(conn.getTransferFromWhere());

                cInfo.setInSync(localSyncStateSet.contains(((AutoSwitchHAConnection) conn).getSlaveId()));

                info.getHaConnectionInfo().add(cInfo);
            }
            info.setInSyncSlaveNums(localSyncStateSet.size() - 1);
        }
        return info;
    }

    public long computeConfirmOffset() {
        final Set<Long> currentSyncStateSet = getSyncStateSet();
        // master的最大offset
        long newConfirmOffset = this.defaultMessageStore.getMaxPhyOffset();

        // 与master建立连接的slave
        List<Long> idList = this.connectionList.stream().map(connection -> ((AutoSwitchHAConnection)connection).getSlaveId()).collect(Collectors.toList());

        // To avoid the syncStateSet is not consistent with connectionList.
        // Fix issue: https://github.com/apache/rocketmq/issues/6662

        // 如果SyncStateSet中，存在未与master建立连接的slave，保持confirmOffset不变
        for (Long syncId : currentSyncStateSet) {
            if (!idList.contains(syncId) && this.brokerControllerId != null && !Objects.equals(syncId, this.brokerControllerId)) {
                LOGGER.warn("Slave {} is still in syncStateSet, but has lost its connection. So new offset can't be compute.", syncId);
                // Without check and re-compute, return the confirmOffset's value directly.
                return this.defaultMessageStore.getConfirmOffsetDirectly();
            }
        }

        // 与master建立连接的slave中，且在SyncStateSet中，最小的slaveOffset
        for (HAConnection connection : this.connectionList) {
            final Long slaveId = ((AutoSwitchHAConnection) connection).getSlaveId();
            if (currentSyncStateSet.contains(slaveId) && connection.getSlaveAckOffset() > 0) {
                newConfirmOffset = Math.min(newConfirmOffset, connection.getSlaveAckOffset());
            }
        }
        return newConfirmOffset;
    }

    public void setSyncStateSet(final Set<Long> syncStateSet) {
        this.writeLock.lock();
        try {
            markSynchronizingSyncStateSetDone();
            this.syncStateSet.clear();
            this.syncStateSet.addAll(syncStateSet);
            this.defaultMessageStore.setConfirmOffset(computeConfirmOffset());
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Return the union of the local and remote syncStateSets
     */
    public Set<Long> getSyncStateSet() {
        this.readLock.lock();
        try {
            if (this.isSynchronizingSyncStateSet) {
                // 如果正在向controller同步，但是还没收到controller回复
                Set<Long> unionSyncStateSet = new HashSet<>(this.syncStateSet.size() + this.remoteSyncStateSet.size());
                unionSyncStateSet.addAll(this.syncStateSet);
                unionSyncStateSet.addAll(this.remoteSyncStateSet);
                return unionSyncStateSet;
            } else {
                // 正常情况
                HashSet<Long> syncStateSet = new HashSet<>(this.syncStateSet.size());
                syncStateSet.addAll(this.syncStateSet);
                return syncStateSet;
            }
        } finally {
            this.readLock.unlock();
        }
    }

    public Set<Long> getLocalSyncStateSet() {
        this.readLock.lock();
        try {
            HashSet<Long> localSyncStateSet = new HashSet<>(this.syncStateSet.size());
            localSyncStateSet.addAll(this.syncStateSet);
            return localSyncStateSet;
        } finally {
            this.readLock.unlock();
        }
    }

    public void truncateEpochFilePrefix(final long offset) {
        this.epochCache.truncatePrefixByOffset(offset);
    }

    public void truncateEpochFileSuffix(final long offset) {
        this.epochCache.truncateSuffixByOffset(offset);
    }

    /**
     * Try to truncate incomplete msg transferred from master.
     */
    public long truncateInvalidMsg() {
        // reput进度未落后于commitlog，消息完整，不需要截断
        long dispatchBehind = this.defaultMessageStore.dispatchBehindBytes();
        if (dispatchBehind <= 0) {
            LOGGER.info("Dispatch complete, skip truncate");
            return -1;
        }

        boolean doNext = true;

        // Here we could use reputFromOffset in DefaultMessageStore directly.
        long reputFromOffset = this.defaultMessageStore.getReputFromOffset();
        do {
            // 根据reput进度向后找最后一条完整的消息
            SelectMappedBufferResult result = this.defaultMessageStore.getCommitLog().getData(reputFromOffset);
            if (result == null) {
                break;
            }

            try {
                reputFromOffset = result.getStartOffset();

                int readSize = 0;
                while (readSize < result.getSize()) {
                    DispatchRequest dispatchRequest = this.defaultMessageStore.getCommitLog().checkMessageAndReturnSize(result.getByteBuffer(), false, false);
                    if (dispatchRequest.isSuccess()) {
                        int size = dispatchRequest.getMsgSize();
                        if (size > 0) {
                            reputFromOffset += size;
                            readSize += size;
                        } else {
                            reputFromOffset = this.defaultMessageStore.getCommitLog().rollNextFile(reputFromOffset);
                            break;
                        }
                    } else {
                        // 找到最后一条完整消息，从这里开始截断
                        doNext = false;
                        break;
                    }
                }
            } finally {
                result.release();
            }
        } while (reputFromOffset < this.defaultMessageStore.getMaxPhyOffset() && doNext);

        LOGGER.info("Truncate commitLog to {}", reputFromOffset);
        // 截断commitlog consumequeue
        this.defaultMessageStore.truncateDirtyFiles(reputFromOffset);
        return reputFromOffset;
    }

    public int getLastEpoch() {
        return this.epochCache.lastEpoch();
    }

    public List<EpochEntry> getEpochEntries() {
        return this.epochCache.getAllEntries();
    }

    public Long getBrokerControllerId() {
        return brokerControllerId;
    }

    public void setBrokerControllerId(Long brokerControllerId) {
        this.brokerControllerId = brokerControllerId;
    }

    class AutoSwitchAcceptSocketService extends AcceptSocketService {

        public AutoSwitchAcceptSocketService(final MessageStoreConfig messageStoreConfig) {
            super(messageStoreConfig);
        }

        @Override
        public String getServiceName() {
            if (defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
                return defaultMessageStore.getBrokerConfig().getIdentifier() + AcceptSocketService.class.getSimpleName();
            }
            return AutoSwitchAcceptSocketService.class.getSimpleName();
        }

        @Override
        protected HAConnection createConnection(SocketChannel sc) throws IOException {
            return new AutoSwitchHAConnection(AutoSwitchHAService.this, sc, AutoSwitchHAService.this.epochCache);
        }
    }
}
