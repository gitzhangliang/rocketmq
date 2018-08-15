package com.zl.rocketmq.docs.Broadcasting;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;


public class ConsumerBroadCastMember2 {
    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer_broadcast");
        consumer.setNamesrvAddr("localhost:9876");
        // 批量消费,每次拉取10条
        consumer.setConsumeMessageBatchMaxSize(10);
        //设置广播消费
        consumer.setMessageModel(MessageModel.BROADCASTING);
        //设置集群消费
//        consumer.setMessageModel(MessageModel.CLUSTERING);
        // 如果非第一次启动，那么按照上次消费的位置继续消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        // 订阅PushTopic下Tag为push的消息
        consumer.subscribe("topic_broadcast", "TagA || Tag B || Tag C");
        consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                System.out.println(Thread.currentThread().getName() + " Receive New Messages: " + new String(msgs.get(0).getBody())+ "   msgId:"+msgs.get(0).getMsgId());

                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.out.println("Consumer2 Started.");

    }
}