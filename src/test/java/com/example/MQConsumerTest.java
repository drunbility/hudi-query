package com.example;


import com.example.dto.BiMessage;
import com.fasterxml.jackson.core.type.TypeReference;
import com.example.jms.RocketMQConsumer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import junit.framework.TestCase;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MQConsumerTest extends TestCase {

    private static final Logger log = LoggerFactory.getLogger(MQConsumerTest.class);

    public static volatile boolean running = true;


    public void testLitePull() throws MQClientException, IOException {

        DefaultLitePullConsumer litePullConsumer = new DefaultLitePullConsumer(
            "bigDataCenterGroup_t1");
        litePullConsumer.setNamesrvAddr("10.233.2.100:9876;10.233.2.101:9876");
        litePullConsumer.subscribe("downloadCenterTopic", "bigDataCenter");
        litePullConsumer.setPullThreadNums(1);
        litePullConsumer.setPullBatchSize(10);
        litePullConsumer.setAutoCommit(false);

        litePullConsumer.start();

        try {
            while (running) {
                List<MessageExt> messageExts = litePullConsumer.poll();
                log.info(messageExts.size() + "========size");

                for (MessageExt m : messageExts) {

                    log.info(m.getMsgId());

                    BiMessage message = RocketMQConsumer.mapper.readValue(m.getBody(),
                        BiMessage.class);

                    LinkedHashMap<String, String> headmap = RocketMQConsumer.mapper.readValue(
                        message.getFieldMap(), new TypeReference<LinkedHashMap<String, String>>() {
                        });

                    log.info(message.toString());


                }
//                litePullConsumer.commitSync();
            }
        } finally {
            litePullConsumer.shutdown();
        }


    }

    public void testAssign() throws MQClientException {
        DefaultLitePullConsumer litePullConsumer = new DefaultLitePullConsumer(
            "bigDataCenterGroup");
        litePullConsumer.setAutoCommit(false);
        litePullConsumer.setNamesrvAddr("10.233.2.100:9876;10.233.2.101:9876");

        litePullConsumer.start();
        Collection<MessageQueue> mqSet = litePullConsumer.fetchMessageQueues("downloadCenterTopic");
        List<MessageQueue> list = new ArrayList<>(mqSet);
        List<MessageQueue> assignList = new ArrayList<>();
        for (int i = 0; i < list.size() / 2; i++) {
            MessageQueue messageQueue = list.get(i);
            assignList.add(messageQueue);
        }
        litePullConsumer.assign(assignList);
//        litePullConsumer.seek(assignList.get(0), 10);
        try {
            while (running) {
                List<MessageExt> messageExts = litePullConsumer.poll();
                System.out.printf("%s %n", messageExts);
//                litePullConsumer.commitSync();
            }
        } finally {
            litePullConsumer.shutdown();
        }
    }

    public void testSimple() throws MQClientException {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(
            "please_rename_unique_group_name_5");
        consumer.setNamesrvAddr("10.233.2.100:9876;10.233.2.101:9876");
        consumer.start();
        try {
            MessageQueue mq = new MessageQueue();
            mq.setQueueId(0);
            mq.setTopic("downloadCenterTopic");
            mq.setBrokerName("bigDataCenter");

            long offset = 0;
            PullResult pullResult = consumer.pull(mq, "bigDataCenter", offset, 15);
            if (pullResult.getPullStatus().equals(PullStatus.FOUND)) {
                System.out.printf("%s%n", pullResult.getMsgFoundList());
                pullResult.getMsgFoundList().forEach(m -> {
                    System.out.println(Arrays.toString(m.getBody()));
                });
//                consumer.updateConsumeOffset(mq, pullResult.getNextBeginOffset());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        consumer.shutdown();
    }

}
