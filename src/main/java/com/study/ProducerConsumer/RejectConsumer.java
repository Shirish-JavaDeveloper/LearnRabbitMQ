package com.study.ProducerConsumer;


import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import java.io.IOException;

public class RejectConsumer extends DefaultConsumer implements BasicConsumer {
    private static String latestMessage = null;
    public RejectConsumer(Channel channel) {
        super(channel);
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        latestMessage = new String(body, "UTF-8");
        System.out.println(" [x] Received '" + latestMessage + "'");
    }

    public String getLatestMessage() {
        return latestMessage;
    }

}

