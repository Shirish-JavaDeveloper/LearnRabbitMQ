package com.study.ProducerConsumer;


import com.rabbitmq.client.Consumer;

public interface BasicConsumer extends Consumer {
     String getLatestMessage();
}
