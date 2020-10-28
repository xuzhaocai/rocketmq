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

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * Average Hashing queue algorithm
 *
 * 平均分配消息队列
 */
public class AllocateMessageQueueAveragely implements AllocateMessageQueueStrategy {
    private final InternalLogger log = ClientLogger.getLog();

    /**
     * 分配mq
     * @param consumerGroup current consumer group  当前消费者组
     * @param currentCID current consumer id        当前消费者id
     * @param mqAll message queue set in current topic  messageQueue列表
     * @param cidAll consumer set in current consumer group    consumer组里面的
     * @return
     */
    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
        List<String> cidAll) {



        //对当前clientId做检查
        if (currentCID == null || currentCID.length() < 1) {
            throw new IllegalArgumentException("currentCID is empty");
        }

        // 对mq列表做检查
        if (mqAll == null || mqAll.isEmpty()) {
            throw new IllegalArgumentException("mqAll is null or mqAll empty");
        }

        // 对所有消费者列表做检查
        if (cidAll == null || cidAll.isEmpty()) {
            throw new IllegalArgumentException("cidAll is null or cidAll empty");
        }

        List<MessageQueue> result = new ArrayList<MessageQueue>();

        // 如果这个 消费者列表中不包含当前这个id ， 就返回个空，因为这个broker可能还不知道这个客户端的存在
        if (!cidAll.contains(currentCID)) {
            log.info("[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {}",
                consumerGroup,
                currentCID,
                cidAll);
            return result;
        }
        // 获取当前 客户端段id 在 所有消费者里面的位置
        int index = cidAll.indexOf(currentCID);

        // 使用所有队列 % 所有消费者
        int mod = mqAll.size() % cidAll.size();




        int averageSize =

            ///mq个数 是否小于等于这个 消费者个数
            mqAll.size() <= cidAll.size() ?
                    1 // mq个数小于消费者个数 一个消费这能平均分一个
                    :
                    (mod > 0 && index < mod
                            ?
                            mqAll.size() / cidAll.size() + 1
                            : mqAll.size() / cidAll.size());




        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
        int range = Math.min(averageSize, mqAll.size() - startIndex);
        for (int i = 0; i < range; i++) {
            result.add(mqAll.get((startIndex + i) % mqAll.size()));
        }
        return result;
    }

    @Override
    public String getName() {
        return "AVG";
    }
}
