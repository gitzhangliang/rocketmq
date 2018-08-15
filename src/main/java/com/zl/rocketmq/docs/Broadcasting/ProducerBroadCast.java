package com.zl.rocketmq.docs.Broadcasting;


import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

public class ProducerBroadCast {
    public static void main(String[] args) {
        DefaultMQProducer producer = new DefaultMQProducer("broadcasting_group");
        producer.setNamesrvAddr("localhost:9876");
        try {
            // 设置实例名称
            producer.setInstanceName("producer_broadcast");
            // 设置重试次数
            //producer.setRetryTimesWhenSendFailed(3);
            // 开启生产者
            producer.start();
            // 创建一条消息
            Message msg = new Message("topic_broadcast", "TagA", "OrderID0034", "message_broadcast_test".getBytes());
            SendResult send = producer.send(msg);
            System.out.println("id:--->" + send.getMsgId() + ",result:--->" + send.getSendStatus());
            
        } catch (MQClientException e) {
            e.printStackTrace();
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (MQBrokerException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } 
        producer.shutdown();
    }
}