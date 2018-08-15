package com.zl.rocketmq.docs.sendModel;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.springframework.util.StopWatch;

public class OnewayProducer {
    public static void main(String[] args) throws Exception{
        //Instantiate with a producer group name.
        DefaultMQProducer producer = new DefaultMQProducer("ExampleProducerGroup");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();
        StopWatch stop = new StopWatch();
        stop.start();
        for (int i = 0; i < 100; i++) {
            //Create a message instance, specifying topic, tag and message body.
            Message msg = new Message("TopicTest" ,
                "TagA", ("Hello RocketMQ " +
                    i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            //Call send message to deliver message to one of brokers.
            producer.sendOneway(msg);

        }
        stop.stop();
        System.out.println("----------------发送一百条消息耗时：" + stop.getTotalTimeMillis());
        producer.shutdown();
    }
}