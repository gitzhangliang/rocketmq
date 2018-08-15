package com.zl.rocketmq.docs.clustering;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.springframework.util.StopWatch;

public class ClusterProducer {

    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("cluster_group_name");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();
        StopWatch stop = new StopWatch();
        stop.start();
        for (int i = 0; i < 100; i++) {
            Message msg = new Message("TopicCluster" ,
                "Tag1" ,("Hello RocketMQ " +i).getBytes(RemotingHelper.DEFAULT_CHARSET)
            );
            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n", sendResult);
        }
        stop.stop();
        System.out.println("----------------发送一百条消息耗时：" + stop.getTotalTimeMillis());
        producer.shutdown();
    }
}