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

package org.apache.rocketmq.broker;

import org.apache.rocketmq.broker.plugin.BrokerAttachedPlugin;
import org.apache.rocketmq.broker.schedule.DelayOffsetSerializeWrapper;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.protocol.BrokerSyncInfo;
import org.apache.rocketmq.remoting.protocol.body.BrokerMemberGroup;
import org.apache.rocketmq.remoting.protocol.body.ConsumerOffsetSerializeWrapper;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.ha.HAConnectionState;
import org.apache.rocketmq.store.ha.HAConnectionStateNotificationRequest;
import org.apache.rocketmq.store.timer.TimerCheckpoint;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class BrokerPreOnlineService extends ServiceThread {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final BrokerController brokerController;

    private int waitBrokerIndex = 0;

    public BrokerPreOnlineService(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public String getServiceName() {
        if (this.brokerController != null && this.brokerController.getBrokerConfig().isInBrokerContainer()) {
            return brokerController.getBrokerIdentity().getIdentifier() + BrokerPreOnlineService.class.getSimpleName();
        }
        return BrokerPreOnlineService.class.getSimpleName();
    }

    @Override
    public void run() {
        LOGGER.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            if (!this.brokerController.isIsolated()) {
                LOGGER.info("broker {} is online", this.brokerController.getBrokerConfig().getCanonicalName());
                break;
            }
            try {
                // 预上线逻辑
                boolean isSuccess = this.prepareForBrokerOnline();
                if (!isSuccess) {
                    this.waitForRunning(1000);
                } else {
                    // 预上线成功，BrokerPreOnlineService线程退出
                    break;
                }
            } catch (Exception e) {
                LOGGER.error("Broker preOnline error, ", e);
            }
        }

        LOGGER.info(this.getServiceName() + " service end");
    }

    CompletableFuture<Boolean> waitForHaHandshakeComplete(String brokerAddr) {
        LOGGER.info("wait for handshake completion with {}", brokerAddr);
        HAConnectionStateNotificationRequest request =
            new HAConnectionStateNotificationRequest(HAConnectionState.TRANSFER, RemotingHelper.parseHostFromAddress(brokerAddr), true);
        if (this.brokerController.getMessageStore().getHaService() != null) {
            this.brokerController.getMessageStore().getHaService().putGroupConnectionStateRequest(request);
        } else {
            LOGGER.error("HAService is null, maybe broker config is wrong. For example, duplicationEnable is true");
            request.getRequestFuture().complete(false);
        }
        return request.getRequestFuture();
    }

    private boolean futureWaitAction(boolean result, BrokerMemberGroup brokerMemberGroup) {
        if (!result) {
            LOGGER.error("wait for handshake completion failed, HA connection lost");
            return false;
        }
        if (this.brokerController.getBrokerConfig().getBrokerId() != MixAll.MASTER_ID) {
            LOGGER.info("slave preOnline complete, start service");
            long minBrokerId = getMinBrokerId(brokerMemberGroup.getBrokerAddrs());
            this.brokerController.startService(minBrokerId, brokerMemberGroup.getBrokerAddrs().get(minBrokerId));
        }
        return true;
    }

    private boolean prepareForMasterOnline(BrokerMemberGroup brokerMemberGroup) {
        List<Long> brokerIdList = new ArrayList<>(brokerMemberGroup.getBrokerAddrs().keySet());
        Collections.sort(brokerIdList);
        // 循环所有slave
        while (true) {
            if (waitBrokerIndex >= brokerIdList.size()) { // 遍历完所有slave
                LOGGER.info("master preOnline complete, start service");
                // 4. 启动二级消息调度，解除隔离
                this.brokerController.startService(MixAll.MASTER_ID, this.brokerController.getBrokerAddr());
                return true;
            }

            String brokerAddrToWait = brokerMemberGroup.getBrokerAddrs().get(brokerIdList.get(waitBrokerIndex));

            // 1. 向slave发送EXCHANGE_BROKER_HA_INFO，同步自己的地址
            try {
                this.brokerController.getBrokerOuterAPI().
                    sendBrokerHaInfo(brokerAddrToWait, this.brokerController.getHAServerAddr(),
                        this.brokerController.getMessageStore().getBrokerInitMaxOffset(), this.brokerController.getBrokerAddr());
            } catch (Exception e) {
                LOGGER.error("send ha address to {} exception, {}", brokerAddrToWait, e);
                return false;
            }

            // 2. 等待 handshake 完成 --- slave与当前master建立连接
            CompletableFuture<Boolean> haHandshakeFuture = waitForHaHandshakeComplete(brokerAddrToWait)
                .thenApply(result -> futureWaitAction(result, brokerMemberGroup));

            try {
                if (!haHandshakeFuture.get()) {
                    return false;
                }
            } catch (Exception e) {
                LOGGER.error("Wait handshake completion exception, {}", e);
                return false;
            }

            // 3. 从slave同步 消费进度 等
            if (syncMetadataReverse(brokerAddrToWait)) {
                waitBrokerIndex++;
            } else {
                return false;
            }
        }
    }

    private boolean syncMetadataReverse(String brokerAddr) {
        try {
            LOGGER.info("Get metadata reverse from {}", brokerAddr);

            // 延迟topic 消费进度
            String delayOffset = this.brokerController.getBrokerOuterAPI().getAllDelayOffset(brokerAddr);
            DelayOffsetSerializeWrapper delayOffsetSerializeWrapper =
                DelayOffsetSerializeWrapper.fromJson(delayOffset, DelayOffsetSerializeWrapper.class);

            // 普通消息topic 消费进度
            ConsumerOffsetSerializeWrapper consumerOffsetSerializeWrapper = this.brokerController.getBrokerOuterAPI().getAllConsumerOffset(brokerAddr);

            TimerCheckpoint timerCheckpoint = this.brokerController.getBrokerOuterAPI().getTimerCheckPoint(brokerAddr);

            if (null != consumerOffsetSerializeWrapper && brokerController.getConsumerOffsetManager().getDataVersion().compare(consumerOffsetSerializeWrapper.getDataVersion()) <= 0) {
                LOGGER.info("{}'s consumerOffset data version is larger than master broker, {}'s consumerOffset will be used.", brokerAddr, brokerAddr);
                this.brokerController.getConsumerOffsetManager().getOffsetTable()
                    .putAll(consumerOffsetSerializeWrapper.getOffsetTable());
                this.brokerController.getConsumerOffsetManager().getDataVersion().assignNewOne(consumerOffsetSerializeWrapper.getDataVersion());
                this.brokerController.getConsumerOffsetManager().persist();
            }

            if (null != delayOffset && brokerController.getScheduleMessageService().getDataVersion().compare(delayOffsetSerializeWrapper.getDataVersion()) <= 0) {
                LOGGER.info("{}'s scheduleMessageService data version is larger than master broker, {}'s delayOffset will be used.", brokerAddr, brokerAddr);
                String fileName =
                    StorePathConfigHelper.getDelayOffsetStorePath(this.brokerController
                        .getMessageStoreConfig().getStorePathRootDir());
                try {
                    MixAll.string2File(delayOffset, fileName);
                    this.brokerController.getScheduleMessageService().load();
                } catch (IOException e) {
                    LOGGER.error("Persist file Exception, {}", fileName, e);
                }
            }

            if (null != this.brokerController.getTimerCheckpoint() && this.brokerController.getTimerCheckpoint().getDataVersion().compare(timerCheckpoint.getDataVersion()) <= 0) {
                LOGGER.info("{}'s timerCheckpoint data version is larger than master broker, {}'s timerCheckpoint will be used.", brokerAddr, brokerAddr);
                this.brokerController.getTimerCheckpoint().setLastReadTimeMs(timerCheckpoint.getLastReadTimeMs());
                this.brokerController.getTimerCheckpoint().setMasterTimerQueueOffset(timerCheckpoint.getMasterTimerQueueOffset());
                this.brokerController.getTimerCheckpoint().getDataVersion().assignNewOne(timerCheckpoint.getDataVersion());
                this.brokerController.getTimerCheckpoint().flush();
            }

            for (BrokerAttachedPlugin brokerAttachedPlugin : brokerController.getBrokerAttachedPlugins()) {
                if (brokerAttachedPlugin != null) {
                    brokerAttachedPlugin.syncMetadataReverse(brokerAddr);
                }
            }

        } catch (Exception e) {
            LOGGER.error("GetMetadataReverse Failed", e);
            return false;
        }

        return true;
    }

    private boolean prepareForSlaveOnline(BrokerMemberGroup brokerMemberGroup) {
        // 1. 请求master broker EXCHANGE_BROKER_HA_INFO 获取ha地址
        BrokerSyncInfo brokerSyncInfo;
        try {
            brokerSyncInfo = this.brokerController.getBrokerOuterAPI()
                .retrieveBrokerHaInfo(brokerMemberGroup.getBrokerAddrs().get(MixAll.MASTER_ID));
        } catch (Exception e) {
            LOGGER.error("retrieve master ha info exception, {}", e);
            return false;
        }

        if (this.brokerController.getMessageStore().getMasterFlushedOffset() == 0
            && this.brokerController.getMessageStoreConfig().isSyncMasterFlushOffsetWhenStartup()) {
            LOGGER.info("Set master flush offset in slave to {}", brokerSyncInfo.getMasterFlushOffset());
            this.brokerController.getMessageStore().setMasterFlushedOffset(brokerSyncInfo.getMasterFlushOffset());
        }

        if (brokerSyncInfo.getMasterHaAddress() != null) {
            // 2-1. master存活 更新master地址
            this.brokerController.getMessageStore().updateHaMasterAddress(brokerSyncInfo.getMasterHaAddress());
            this.brokerController.getMessageStore().updateMasterAddress(brokerSyncInfo.getMasterAddress());
        } else {
            LOGGER.info("fetch master ha address return null, start service directly");
            long minBrokerId = getMinBrokerId(brokerMemberGroup.getBrokerAddrs());
            this.brokerController.startService(minBrokerId, brokerMemberGroup.getBrokerAddrs().get(minBrokerId));
            return true;
        }

        // 3. master存活 等待handshake完成 --- DefaultHAClient/Connection 只需要建立连接即可
        CompletableFuture<Boolean> haHandshakeFuture = waitForHaHandshakeComplete(brokerSyncInfo.getMasterHaAddress())
            .thenApply(result -> futureWaitAction(result, brokerMemberGroup));

        try {
            if (!haHandshakeFuture.get()) {
                return false;
            }
        } catch (Exception e) {
            LOGGER.error("Wait handshake completion exception, {}", e);
            return false;
        }

        return true;
    }

    private boolean prepareForBrokerOnline() {

        // 从nameserver获取broker组成员
        BrokerMemberGroup brokerMemberGroup;
        try {
            brokerMemberGroup = this.brokerController.getBrokerOuterAPI().syncBrokerMemberGroup(
                this.brokerController.getBrokerConfig().getBrokerClusterName(),
                this.brokerController.getBrokerConfig().getBrokerName(),
                this.brokerController.getBrokerConfig().isCompatibleWithOldNameSrv());
        } catch (Exception e) {
            LOGGER.error("syncBrokerMemberGroup from namesrv error, start service failed, will try later, ", e);
            return false;
        }

        if (brokerMemberGroup != null && !brokerMemberGroup.getBrokerAddrs().isEmpty()) {
            // broker组内有成员存活

            long minBrokerId = getMinBrokerId(brokerMemberGroup.getBrokerAddrs());

            if (this.brokerController.getBrokerConfig().getBrokerId() == MixAll.MASTER_ID) {
                // 当前实例是master
                return prepareForMasterOnline(brokerMemberGroup);
            } else if (minBrokerId == MixAll.MASTER_ID) {
                // 当前实例是slave，且master存活
                return prepareForSlaveOnline(brokerMemberGroup);
            } else {
                // 当前实例是slave，但是master下线，如果自己是最小broker才能作为代理master上线
                LOGGER.info("no master online, start service directly");
                this.brokerController.startService(minBrokerId, brokerMemberGroup.getBrokerAddrs().get(minBrokerId));
            }
        } else {
            // broker组无存活实例
            LOGGER.info("no other broker online, will start service directly");
            this.brokerController.startService(this.brokerController.getBrokerConfig().getBrokerId(), this.brokerController.getBrokerAddr());
        }

        return true;
    }

    private long getMinBrokerId(Map<Long, String> brokerAddrMap) {
        Map<Long, String> brokerAddrMapCopy = new HashMap<>(brokerAddrMap);
        brokerAddrMapCopy.remove(this.brokerController.getBrokerConfig().getBrokerId());
        if (!brokerAddrMapCopy.isEmpty()) {
            return Collections.min(brokerAddrMapCopy.keySet());
        }
        return this.brokerController.getBrokerConfig().getBrokerId();
    }
}
