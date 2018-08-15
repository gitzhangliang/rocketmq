package com.zl.rocketmq.docs.sendModel;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.springframework.util.StopWatch;

/**可靠的异步传输
 *应用：异步传输通常用于响应时间敏感的业务场景
 *
 */
public class AsyncProducer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("ExampleProducerGroup");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();
        producer.setRetryTimesWhenSendAsyncFailed(0);
        StopWatch stop = new StopWatch();
        stop.start();
        for (int i = 0; i < 100; i++) {
                final int index = i;
                Message msg = new Message("TopicTest",
                    "TagA",
                    "OrderID188",
                        ("Hello RocketMQ " +i).getBytes(RemotingHelper.DEFAULT_CHARSET));
                producer.send(msg, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        System.out.printf("%-10d OK %s %n", index,
                            sendResult.getMsgId());
                    }
                    @Override
                    public void onException(Throwable e) {
                        System.out.printf("%-10d Exception %s %n", index, e);
                        e.printStackTrace();
                    }
                });
        }
        stop.stop();
        System.out.println("----------------发送一百条消息耗时：" + stop.getTotalTimeMillis());
        producer.shutdown();
    }
}