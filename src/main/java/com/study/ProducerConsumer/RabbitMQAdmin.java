package com.study.ProducerConsumer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;

public class RabbitMQAdmin {
    public static final String DIRECT_EXCHANGE_TYPE = "direct";
    public static final String DEFAULT_ROUTING_KEY = "default_Routing_Key";
    private final Connection connection;
    private  Channel channel;

    public RabbitMQAdmin(String hostName) throws IOException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(hostName);
        connection = factory.newConnection();
        channel = connection.createChannel();
    }


    public Connection getConnection() {
        return connection;
    }

    public void closeConnection() throws IOException {
        connection.close();
    }

    public Channel createQueue(String queueName) throws IOException {
        channel.queueDeclare(queueName, true, false, false, null);
        return channel;
    }

    public boolean sendMessage(String queueName, byte[] message) throws IOException {
            createQueue(queueName);
            channel.basicPublish("",queueName,null,message);
            return true;
    }

    public boolean sendMessageTo(String exchange, byte[] message) throws IOException {
        channel.basicPublish(exchange,DEFAULT_ROUTING_KEY,null,message);
        return true;
    }
    public boolean sendMessageTo(String exchange,String routingKey, byte[] message) throws IOException {
        channel.basicPublish(exchange,routingKey,null,message);
        return true;
    }

    public Channel createTempQueue(String queueName) throws IOException {
        return createQueue(queueName,false,true);
    }

    public Channel createQueueWhichDiesAfterServerRestart(String queueName) throws IOException {
        return createQueue(queueName,false,false);
    }


    public Channel createQueue(String queueName,boolean durable,boolean autoDelete) throws IOException {
        channel.queueDeclare(queueName, durable, false, autoDelete, null);
        return channel;
    }

    public Channel createDurableQueue(String queueName) throws IOException {
        return createQueue(queueName,true,false);
    }

    public String consumeMessageFrom(String queueName) throws IOException, InterruptedException {
        BasicConsumer consumer = new SimpleMessageConsumer(channel);
        return consumeMessageWith(consumer,queueName);
    }

    public String consumeMessageWith(BasicConsumer consumer , String queueName) throws IOException, InterruptedException {
        channel.basicConsume(queueName,true,consumer);
        Thread.sleep(1000);
        return consumer.getLatestMessage();
    }

    public String consumeMessageWithNackConsumer(String queueName,boolean requeue) throws IOException, InterruptedException {
        BasicConsumer  consumer=  new NackConsumer(channel,requeue);
        channel.basicConsume(queueName,false,consumer);
        Thread.sleep(1000);
        return  consumer.getLatestMessage();
    }


    public Channel createAndBindExchangeToQueue(String exchange, String queue) throws IOException {
        createAndBindExchangeToQueue(exchange, queue,DIRECT_EXCHANGE_TYPE);
        return channel;
    }

    public void createAndBindExchangeToQueue(String exchange, String queue,String type) throws IOException {
        String routingKey = DEFAULT_ROUTING_KEY;
        createAndBind(exchange, queue, type, routingKey);
    }

    private void createAndBind(String exchange, String queue, String type, String routingKey) throws IOException {
        channel.exchangeDeclare(exchange, type);
        channel.queueDeclare(queue, true, false, false, null);
        channel.queueBind(queue,exchange,routingKey );
    }

    public void createAndBindExchangeToQueue(String exchange, String queueName, String exchangeType, String routingKey) throws IOException {
        createAndBind(exchange, queueName, exchangeType, routingKey);
    }


    public void deleteExchange(String exchangeName) throws IOException {
        channel.exchangeDelete(exchangeName);
    }
}


