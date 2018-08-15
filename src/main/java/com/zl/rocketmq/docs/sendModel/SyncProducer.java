package com.zl.rocketmq.docs.sendModel;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.StopWatch;

/**可靠的同步传输
 *应用：可靠的同步传输用于广泛的场景，如重要的通知消息，短信通知，短信营销系统等。
 *
 */
public class SyncProducer {

    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();
        StopWatch stop = new StopWatch();
        stop.start();
        for (int i = 0; i < 100; i++) {
            Message msg = new Message("TopicTest" ,
                "TagA" ,("Hello RocketMQ " +i).getBytes(RemotingHelper.DEFAULT_CHARSET)
            );
            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n", sendResult);
        }
        stop.stop();
        System.out.println("----------------发送一百条消息耗时：" + stop.getTotalTimeMillis());
        producer.shutdown();
    }
}