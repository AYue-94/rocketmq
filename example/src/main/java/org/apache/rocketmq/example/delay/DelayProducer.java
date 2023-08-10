package org.apache.rocketmq.example.delay;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class DelayProducer {
    public static void main(String[] args) throws MQClientException, InterruptedException {
        int messages = 10;
        CountDownLatch countDownLatch = new CountDownLatch(messages);

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("delay_message_consumer01");
        consumer.setNamesrvAddr("localhost:9876");
        consumer.subscribe("MyTestTopicA", "*");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                System.out.printf("%s, %s Receive New Messages: %s %n", new Date(), Thread.currentThread().getName(), msgs);
                countDownLatch.countDown();
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.out.printf("Consumer Started.%n");

        DefaultMQProducer producer = new DefaultMQProducer("delay-producer01");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();
        for (int i = 0; i < messages; i++) {
            try {
                Message msg = new Message(
                        "MyTestTopicA",
                        "TagA",
                        ("Hello Delay " + i).getBytes(StandardCharsets.UTF_8)
                );
                msg.setDelayTimeLevel(3); // 10s
                SendResult sendResult = producer.send(msg);


                System.out.printf("%s, %s%n", new Date(), sendResult);
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1000);
            }
        }
        producer.shutdown();

        countDownLatch.await();

        consumer.shutdown();
    }
}
