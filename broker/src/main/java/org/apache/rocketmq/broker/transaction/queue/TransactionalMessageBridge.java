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
package org.apache.rocketmq.broker.transaction.queue;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.logging.InnerLoggerFactory;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TransactionalMessageBridge {
    private static final InternalLogger LOGGER = InnerLoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    private final ConcurrentHashMap<MessageQueue, MessageQueue> opQueueMap = new ConcurrentHashMap<>();
    private final BrokerController brokerController;
    private final MessageStore store;
    private final SocketAddress storeHost;

    public TransactionalMessageBridge(BrokerController brokerController, MessageStore store) {
        try {
            this.brokerController = brokerController;
            this.store = store;
            this.storeHost =
                new InetSocketAddress(brokerController.getBrokerConfig().getBrokerIP1(),
                    brokerController.getNettyServerConfig().getListenPort());
        } catch (Exception e) {
            LOGGER.error("Init TransactionBridge error", e);
            throw new RuntimeException(e);
        }

    }
    // 查找消费位置
    public long fetchConsumeOffset(MessageQueue mq) {
        long offset = brokerController.getConsumerOffsetManager().queryOffset(TransactionalMessageUtil.buildConsumerGroup(),
            mq.getTopic(), mq.getQueueId());
        if (offset == -1) {// 如果是-1 。就从存储层那获取
            offset = store.getMinOffsetInQueue(mq.getTopic(), mq.getQueueId());
        }
        return offset;
    }

    public Set<MessageQueue> fetchMessageQueues(String topic) {
        Set<MessageQueue> mqSet = new HashSet<>();

        // 获取topic
        TopicConfig topicConfig = selectTopicConfig(topic);
        if (topicConfig != null && topicConfig.getReadQueueNums() > 0) {
            for (int i = 0; i < topicConfig.getReadQueueNums(); i++) {
                MessageQueue mq = new MessageQueue();
                mq.setTopic(topic);
                mq.setBrokerName(brokerController.getBrokerConfig().getBrokerName());
                mq.setQueueId(i);
                mqSet.add(mq);
            }
        }
        return mqSet;
    }

    public void updateConsumeOffset(MessageQueue mq, long offset) {
        this.brokerController.getConsumerOffsetManager().commitOffset(
            RemotingHelper.parseSocketAddressAddr(this.storeHost), TransactionalMessageUtil.buildConsumerGroup(), mq.getTopic(),
            mq.getQueueId(), offset);
    }

    public PullResult getHalfMessage(int queueId, long offset, int nums) {
        String group = TransactionalMessageUtil.buildConsumerGroup();
        String topic = TransactionalMessageUtil.buildHalfTopic();
        SubscriptionData sub = new SubscriptionData(topic, "*");
        return getMessage(group, topic, queueId, offset, nums, sub);
    }

    public PullResult getOpMessage(int queueId, long offset, int nums) {

        ///消费者组 ：CID_SYS_RMQ_TRANS
        String group = TransactionalMessageUtil.buildConsumerGroup();
        //topic： RMQ_SYS_TRANS_OP_HALF_TOPIC
        String topic = TransactionalMessageUtil.buildOpTopic();


        // 生成订阅data
        SubscriptionData sub = new SubscriptionData(topic, "*");
        return getMessage(group, topic, queueId, offset, nums, sub);
    }

    /**
     * 获取消息
     * @param group  消费者组
     * @param topic  topic
     * @param queueId  mq id
     * @param offset  偏移量
     * @param nums  获取数量
     * @param sub  订阅data
     * @return
     */
    private PullResult getMessage(String group, String topic, int queueId, long offset, int nums,
        SubscriptionData sub) {


        /// 从存储器里面获取消息
        GetMessageResult getMessageResult = store.getMessage(group, topic, queueId, offset, nums, null);

        if (getMessageResult != null) {
            PullStatus pullStatus = PullStatus.NO_NEW_MSG;
            List<MessageExt> foundList = null;
            switch (getMessageResult.getStatus()) {
                case FOUND:
                    pullStatus = PullStatus.FOUND;
                    foundList = decodeMsgList(getMessageResult);
                    this.brokerController.getBrokerStatsManager().incGroupGetNums(group, topic,
                        getMessageResult.getMessageCount());
                    this.brokerController.getBrokerStatsManager().incGroupGetSize(group, topic,
                        getMessageResult.getBufferTotalSize());
                    this.brokerController.getBrokerStatsManager().incBrokerGetNums(getMessageResult.getMessageCount());
                    this.brokerController.getBrokerStatsManager().recordDiskFallBehindTime(group, topic, queueId,
                        this.brokerController.getMessageStore().now() - foundList.get(foundList.size() - 1)
                            .getStoreTimestamp());
                    break;
                case NO_MATCHED_MESSAGE:
                    pullStatus = PullStatus.NO_MATCHED_MSG;
                    LOGGER.warn("No matched message. GetMessageStatus={}, topic={}, groupId={}, requestOffset={}",
                        getMessageResult.getStatus(), topic, group, offset);
                    break;
                case NO_MESSAGE_IN_QUEUE:
                    pullStatus = PullStatus.NO_NEW_MSG;
                    LOGGER.warn("No new message. GetMessageStatus={}, topic={}, groupId={}, requestOffset={}",
                        getMessageResult.getStatus(), topic, group, offset);
                    break;
                case MESSAGE_WAS_REMOVING:
                case NO_MATCHED_LOGIC_QUEUE:
                case OFFSET_FOUND_NULL:
                case OFFSET_OVERFLOW_BADLY:
                case OFFSET_OVERFLOW_ONE:
                case OFFSET_TOO_SMALL:
                    pullStatus = PullStatus.OFFSET_ILLEGAL;
                    LOGGER.warn("Offset illegal. GetMessageStatus={}, topic={}, groupId={}, requestOffset={}",
                        getMessageResult.getStatus(), topic, group, offset);
                    break;
                default:
                    assert false;
                    break;
            }

            return new PullResult(pullStatus, getMessageResult.getNextBeginOffset(), getMessageResult.getMinOffset(),
                getMessageResult.getMaxOffset(), foundList);

        } else {
            LOGGER.error("Get message from store return null. topic={}, groupId={}, requestOffset={}", topic, group,
                offset);
            return null;
        }
    }

    private List<MessageExt> decodeMsgList(GetMessageResult getMessageResult) {
        List<MessageExt> foundList = new ArrayList<>();
        try {
            List<ByteBuffer> messageBufferList = getMessageResult.getMessageBufferList();
            for (ByteBuffer bb : messageBufferList) {
                MessageExt msgExt = MessageDecoder.decode(bb);
                foundList.add(msgExt);
            }

        } finally {
            getMessageResult.release();
        }

        return foundList;
    }

    /**
     * 将half消息存储起来
     * @param messageInner
     * @return
     */
    public PutMessageResult putHalfMessage(MessageExtBrokerInner messageInner) {

        // 进行存储
        return store.putMessage(parseHalfMessageInner(messageInner));
    }

    private MessageExtBrokerInner parseHalfMessageInner(MessageExtBrokerInner msgInner) {


        // 设置两个属性值，一个是 real_topic 就是这个事务消息真实的topic
        MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_REAL_TOPIC, msgInner.getTopic());
        // 一个是real_qid 就是这个事务消息真实的队列id
        MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_REAL_QUEUE_ID,
            String.valueOf(msgInner.getQueueId()));



        msgInner.setSysFlag(
            MessageSysFlag.resetTransactionValue(msgInner.getSysFlag(), MessageSysFlag.TRANSACTION_NOT_TYPE));

        ///设置 topic 是这个  RMQ_SYS_TRANS_HALF_TOPIC
        msgInner.setTopic(TransactionalMessageUtil.buildHalfTopic());

        // 设置queue  id 是0
        msgInner.setQueueId(0);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        return msgInner;
    }

    /**
     * 将消息 put到op topic 里面
     * @param messageExt  这个消息是 从half topic 取出来，然后转成原来的消息，又将原来的消息存储成功后 ，这个是 half topic 取出来的准备阶段的消息
     * @param opType 操作类型
     * @return
     */
    public boolean putOpMessage(MessageExt messageExt, String opType) {

        // 封装msg queue
        MessageQueue messageQueue = new MessageQueue(messageExt.getTopic(),
            this.brokerController.getBrokerConfig().getBrokerName(), messageExt.getQueueId());


        // d
        if (TransactionalMessageUtil.REMOVETAG.equals(opType)) {
            return addRemoveTagInTransactionOp(messageExt, messageQueue);
        }
        return true;
    }

    public PutMessageResult putMessageReturnResult(MessageExtBrokerInner messageInner) {
        LOGGER.debug("[BUG-TO-FIX] Thread:{} msgID:{}", Thread.currentThread().getName(), messageInner.getMsgId());
        return store.putMessage(messageInner);
    }

    /**
     * 存储消息
     * @param messageInner
     * @return
     */
    public boolean putMessage(MessageExtBrokerInner messageInner) {
        // 进行存储
        PutMessageResult putMessageResult = store.putMessage(messageInner);
        if (putMessageResult != null
            && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
            return true;
        } else {
            LOGGER.error("Put message failed, topic: {}, queueId: {}, msgId: {}",
                messageInner.getTopic(), messageInner.getQueueId(), messageInner.getMsgId());
            return false;
        }
    }

    public MessageExtBrokerInner renewImmunityHalfMessageInner(MessageExt msgExt) {
        MessageExtBrokerInner msgInner = renewHalfMessageInner(msgExt);
        String queueOffsetFromPrepare = msgExt.getUserProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET);
        if (null != queueOffsetFromPrepare) {
            MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET,
                String.valueOf(queueOffsetFromPrepare));
        } else {
            MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET,
                String.valueOf(msgExt.getQueueOffset()));
        }

        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));

        return msgInner;
    }
    /// 重新创建一个 HalfMessage
    public MessageExtBrokerInner renewHalfMessageInner(MessageExt msgExt) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(msgExt.getTopic());
        msgInner.setBody(msgExt.getBody());
        msgInner.setQueueId(msgExt.getQueueId());
        msgInner.setMsgId(msgExt.getMsgId());
        msgInner.setSysFlag(msgExt.getSysFlag());
        msgInner.setTags(msgExt.getTags());


        msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(msgInner.getTags()));
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        msgInner.setWaitStoreMsgOK(false);
        return msgInner;
    }

    /**
     * 封装op message
     * @param message
     * @param messageQueue
     * @return
     */
    private MessageExtBrokerInner makeOpMessageInner(Message message, MessageQueue messageQueue) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(message.getTopic());
        msgInner.setBody(message.getBody());
        msgInner.setQueueId(messageQueue.getQueueId());
        msgInner.setTags(message.getTags());
        msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(msgInner.getTags()));
        msgInner.setSysFlag(0);
        MessageAccessor.setProperties(msgInner, message.getProperties());
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(message.getProperties()));
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setBornHost(this.storeHost);
        msgInner.setStoreHost(this.storeHost);
        msgInner.setWaitStoreMsgOK(false);
        MessageClientIDSetter.setUniqID(msgInner);
        return msgInner;
    }

    private TopicConfig selectTopicConfig(String topic) {
        TopicConfig topicConfig = brokerController.getTopicConfigManager().selectTopicConfig(topic);
        if (topicConfig == null) {
            topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(
                topic, 1, PermName.PERM_WRITE | PermName.PERM_READ, 0);
        }
        return topicConfig;
    }

    /**
     * Use this function while transaction msg is committed or rollback write a flag 'd' to operation queue for the
     * msg's offset
     * 添加删除标识到 op topic
     * @param messageExt Op message
     * @param messageQueue Op message queue
     * @return This method will always return true.
     */
    private boolean addRemoveTagInTransactionOp(MessageExt messageExt, MessageQueue messageQueue) {

        // 封装一个删除消息标识
        /// 里面内容： op topic  ，tag 是删除d   消息内容是 queueOffset
        Message message = new Message(TransactionalMessageUtil.buildOpTopic(), TransactionalMessageUtil.REMOVETAG,
            String.valueOf(messageExt.getQueueOffset()).getBytes(TransactionalMessageUtil.charset));
        writeOp(message, messageQueue);
        return true;
    }

    /**
     * 将消息写到op topic 中
     * @param message  消息
     * @param mq  mq
     */
    private void writeOp(Message message, MessageQueue mq) {

        /// 创建mq
        MessageQueue opQueue;
        if (opQueueMap.containsKey(mq)) {
            opQueue = opQueueMap.get(mq);
        } else {
            opQueue = getOpQueueByHalf(mq);
            MessageQueue oldQueue = opQueueMap.putIfAbsent(mq, opQueue);
            if (oldQueue != null) {
                opQueue = oldQueue;
            }
        }


        if (opQueue == null) {
            opQueue = new MessageQueue(TransactionalMessageUtil.buildOpTopic(), mq.getBrokerName(), mq.getQueueId());
        }


        putMessage(makeOpMessageInner(message, opQueue));
    }

    private MessageQueue getOpQueueByHalf(MessageQueue halfMQ) {
        MessageQueue opQueue = new MessageQueue();
        opQueue.setTopic(TransactionalMessageUtil.buildOpTopic());
        opQueue.setBrokerName(halfMQ.getBrokerName());
        opQueue.setQueueId(halfMQ.getQueueId());
        return opQueue;
    }
    // 根据offset从存储器获取msg
    public MessageExt lookMessageByOffset(final long commitLogOffset) {
        return this.store.lookMessageByOffset(commitLogOffset);
    }

    public BrokerController getBrokerController() {
        return brokerController;
    }
}
