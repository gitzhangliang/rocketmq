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
public class ClusterConsumer3 {
    private static volatile int index = 0;
    private  static String syn = "11";
    public static void main(String[] args) {
        //不同组
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("cluster_consumer_group1");
        consumer.setNamesrvAddr("localhost:9876");
        try {
            consumer.subscribe("TopicCluster", "Tag1");
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            consumer.registerMessageListener((MessageListenerConcurrently) (list, context) -> {
                try {
                    for (MessageExt messageExt : list) {
                        synchronized (syn) {
                            index++;
                            String messageBody = new String(messageExt.getBody(), RemotingHelper.DEFAULT_CHARSET);
                            System.out.println(Thread.currentThread().getName() + "  消费响应：msgId : " + messageExt.getMsgId() + ",  msgBody : " + messageBody);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
                System.out.println("ClusterConsumer3消费消息数量："+index);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            });
            consumer.start();
        } catch (Exception e) {
        }
        System.out.println("Consumer3 Started.");
    }
}
