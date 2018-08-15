package com.zl.rocketmq.transaction;


import org.apache.rocketmq.client.producer.LocalTransactionExecuter;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.common.message.Message;

/**
 * 执行本地事务 
 */  
public class TransactionExecuterImpl implements LocalTransactionExecuter {
    // private AtomicInteger transactionIndex = new AtomicInteger(1);  
  
    public LocalTransactionState executeLocalTransactionBranch(final Message msg, final Object arg) {
  
        System.out.println("执行本地事务msg = " + new String(msg.getBody()));  
  
        System.out.println("执行本地事务arg = " + arg);  
  
        String tags = msg.getTags();  
  
        if (tags.equals("transaction2")) {  
            System.out.println("======我的操作============，失败了  -进行ROLLBACK");  
            return LocalTransactionState.ROLLBACK_MESSAGE;  
        }  
        return LocalTransactionState.COMMIT_MESSAGE;  
        // return LocalTransactionState.UNKNOW;  
    }  
}