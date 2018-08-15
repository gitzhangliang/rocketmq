package com.zl.rocketmq.docs.clustering;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * Created by tzxx on 2018/8/15.
 */
public class ClusterConsumer2 {
    private static volatile int index = 0;
    private  static String sync = "11";
    public static void main(String[] args) {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("cluster_consumer_group");
        consumer.setNamesrvAddr("localhost:9876");
        try {
            consumer.subscribe("TopicCluster", "Tag1");
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            consumer.registerMessageListener((MessageListenerConcurrently) (list, context) -> {
                try {
                    for (MessageExt messageExt : list) {
                        synchronized (sync) {
                            index++;
                            String messageBody = new String(messageExt.getBody(), RemotingHelper.DEFAULT_CHARSET);
                            System.out.println(Thread.currentThread().getName() + "  消费响应：msgId : " + messageExt.getMsgId() + ",  msgBody : " + messageBody);
                        }
                    }
                } catch (Exception e) {
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
                System.out.println("ClusterConsumer2消费消息数量："+index);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            });
            consumer.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Consumer2 Started.");

    }
}
