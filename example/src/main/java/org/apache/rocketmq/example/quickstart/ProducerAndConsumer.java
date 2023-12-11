package org.apache.rocketmq.example.quickstart;

import org.apache.rocketmq.client.exception.MQClientException;

public class ProducerAndConsumer {
    public static void main(String[] args) throws MQClientException, InterruptedException {
        Consumer.main(null);
        Producer.main(null);
    }
}
