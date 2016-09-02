package com.study.ProducerConsumer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import java.io.IOException;

public class SimpleMessageConsumer extends DefaultConsumer implements BasicConsumer {
    private static String latestMessage = null;
    public SimpleMessageConsumer(Channel channel) {
        super(channel);
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] message) throws IOException {
        latestMessage = new String(message, "UTF-8");
        System.out.println(" [x] Received '" + latestMessage + "'");
    }

    public String getLatestMessage() {
        return latestMessage;
    }
}
