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
package org.apache.rocketmq.client.consumer.rebalance;

import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.InternalLogger;

import java.util.ArrayList;
import java.util.List;

/**
 * Average Hashing queue algorithm
 */
public class AllocateMessageQueueAveragely implements AllocateMessageQueueStrategy {
    private final InternalLogger log = ClientLogger.getLog();

    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
        List<String> cidAll) {
        if (currentCID == null || currentCID.length() < 1) {
            throw new IllegalArgumentException("currentCID is empty");
        }
        if (mqAll == null || mqAll.isEmpty()) {
            throw new IllegalArgumentException("mqAll is null or mqAll empty");
        }
        if (cidAll == null || cidAll.isEmpty()) {
            throw new IllegalArgumentException("cidAll is null or cidAll empty");
        }

        List<MessageQueue> result = new ArrayList<MessageQueue>();
        if (!cidAll.contains(currentCID)) {
            log.info("[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {}",
                consumerGroup,
                currentCID,
                cidAll);
            return result;
        }

        int index = cidAll.indexOf(currentCID);
        int mod = mqAll.size() % cidAll.size();
        int averageSize =
            mqAll.size() <= cidAll.size() ? 1 : (mod > 0 && index < mod ? mqAll.size() / cidAll.size()
                + 1 : mqAll.size() / cidAll.size());
        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
        int range = Math.min(averageSize, mqAll.size() - startIndex);
        for (int i = 0; i < range; i++) {
            result.add(mqAll.get((startIndex + i) % mqAll.size()));
        }
        return result;
    }

    // 2个queue
    // 3个client

    // index = 0
    // mod = 2
    // averageSize = 1
    // startIndex = index * averageSize = 0
    // range = min(1,2-0)=1
    // result = (0+0)%2=0

    // index = 1
    // mod = 2
    // averageSize = 1
    // startIndex = index * averageSize = 1
    // range = min(1,2-1)=1
    // result = (1+0)%2=1

    // index = 2
    // mod = 2
    // averageSize = 1
    // startIndex = index * averageSize + mod = 5
    // range = min(1,2-5)=-3
    // result =

    // 8个queue
    // 3个client 0 1 2

    // index = 0
    // mod = 2
    // averageSize = 8/3 + 1 = 3
    // startIndex = index * averageSize = 0 * 3 = 0
    // range = min(3,8-3)=3
    // result = (0+0)%8=0,(0+1)%8=1,(0+2)%8=2

    // index = 1
    // mod = 2
    // averageSize = 8/3+1 = 3
    // startIndex = index * averageSize = 3
    // range = min(3,8-3)=3
    // result = (3+0)%8=3,(3+1)%8=4,(3+2)%8=5,

    // index = 2
    // mod = 2
    // averageSize = 8/3 = 2
    // startIndex = index * averageSize + mod = 6
    // range = min(2,8-6)=2
    // result = (6+0)%8=6,(6+1)%8=7

    @Override
    public String getName() {
        return "AVG";
    }
}
