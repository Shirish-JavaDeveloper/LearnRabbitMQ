package com.study.ProducerConsumer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.apache.activemq.artemis.api.core.BroadcastEndpointFactory;
import org.apache.activemq.artemis.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.hornetq.api.core.UDPBroadcastGroupConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.io.IOException;
import java.util.*;
import java.util.Queue;

import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.assertNotNull;

public class RabbitMQTest {
    private static final String TEST_QUEUE = "TestQueue";
    private static final String TEST_QUEUE_2 = "TestQueue_2";

    private static final String SAMPLE_MESSAGE_TEXT = "SampleMessage";
    private static final String SAMPLE_MESSAGE_TEXT_2 = "SampleMessage 2";
    private static final String TEST_EXCHANGE = "TEST_EXCHANGE";
    private static final String TEST_FANOUT_EXCHANGE = "TEST_FANOUT_EXCHANGE";
    private static final String TEST_QUEUE_3 = "TestQueue3";
    private static final String TEST_EXCHANGE_1 = "TEST_EXCHANGE_1";
    private static final String TEST_FANOUT_QUEUE = "TestFanoutQueue";
    private static final String TEST_FANOUT_QUEUE_2 = "TestFanoutQueue_2";
    private static final String TEST_FANOUT_QUEUE_3 = "TestFanoutQueue_3";
    private static final String TEST_TOPIC_QUEUE = "TestTopicQueue";
    private static final String TEST_TOPIC_QUEUE_2 = "TestTopicQueue_2";
    private static final String TEST_TOPIC_QUEUE_3 = "TestTopicQueue_3";
    private static final String TEST_TOPIC_EXCHANGE = "TEST_TOPIC_EXCHANGE";

    private RabbitMQAdmin admin = null ;

    @Before
    public void setUp() throws IOException {
        admin = new RabbitMQAdmin("localhost");
    }

    @After
    public void tearDown() throws IOException {
        if(null!= admin) {
            admin.closeConnection();
        }
    }

    @Test
    public void shouldBeAbleToConnectToRabbitMQServer(){
        Connection connection = admin.getConnection();
        assertEquals("localhost",connection.getHost());
    }


    @Test
    public void sendMessage() throws IOException, InterruptedException {
        admin.sendMessage(TEST_QUEUE, SAMPLE_MESSAGE_TEXT.getBytes());
    }


    @Test
    public void sendAndReceiveMessageFromQueue() throws IOException, InterruptedException {
        String message = admin.consumeMessageFrom(TEST_QUEUE);

        assertEquals(SAMPLE_MESSAGE_TEXT, message);
    }

    @Test
    public void bindQueuesToExchanges() throws IOException, InterruptedException {
        admin.createAndBindExchangeToQueue(TEST_EXCHANGE,TEST_QUEUE,"direct","Computer");
        admin.createAndBindExchangeToQueue(TEST_EXCHANGE,TEST_QUEUE_2,"direct","Mechanical");
        admin.createAndBindExchangeToQueue(TEST_EXCHANGE,TEST_QUEUE_3,"direct","Electrical");
    }

    @Test
    public void sendMessageToExchange() throws IOException, InterruptedException {
        admin.sendMessageTo(TEST_EXCHANGE,"Mechanical", SAMPLE_MESSAGE_TEXT_2.getBytes());

    }

    @Test
    public void consumeMessageFromQueue() throws IOException, InterruptedException {

        String message = admin.consumeMessageFrom(TEST_QUEUE_2);

        assertEquals(SAMPLE_MESSAGE_TEXT_2, message);
    }


    @Test
    public void bindQueuesToFanoutExchange() throws IOException, InterruptedException {
        admin.createAndBindExchangeToQueue(TEST_FANOUT_EXCHANGE , TEST_FANOUT_QUEUE,"fanout","Computer");
        admin.createAndBindExchangeToQueue(TEST_FANOUT_EXCHANGE, TEST_FANOUT_QUEUE_2,"fanout","Mechanical");
        admin.createAndBindExchangeToQueue(TEST_FANOUT_EXCHANGE, TEST_FANOUT_QUEUE_3,"fanout","Electrical");
    }

    @Test
    public void sendMessageToFanoutExchange() throws IOException, InterruptedException {
        admin.sendMessageTo(TEST_FANOUT_EXCHANGE,"Electrical", SAMPLE_MESSAGE_TEXT_2.getBytes());

    }

    @Test
    public void consumeMessageFromFanoutQueues() throws IOException, InterruptedException {
        String message = admin.consumeMessageFrom(TEST_FANOUT_QUEUE);

        assertEquals(SAMPLE_MESSAGE_TEXT, message);

        message = admin.consumeMessageFrom(TEST_FANOUT_QUEUE_2);

        assertEquals(SAMPLE_MESSAGE_TEXT, message);

        message = admin.consumeMessageFrom(TEST_FANOUT_QUEUE_3);

        assertEquals(SAMPLE_MESSAGE_TEXT, message);
    }


    @Test
    public void bindQueuesToTopicExchange() throws IOException, InterruptedException {
        admin.createAndBindExchangeToQueue(TEST_TOPIC_EXCHANGE , TEST_TOPIC_QUEUE,"topic","*.critical");
        admin.createAndBindExchangeToQueue(TEST_TOPIC_EXCHANGE, TEST_TOPIC_QUEUE_2,"topic","Log.#");
        admin.createAndBindExchangeToQueue(TEST_TOPIC_EXCHANGE, TEST_TOPIC_QUEUE_3,"topic","Log.debug.*");
    }

    @Test
    public void sendMessageToTopicExchange() throws IOException, InterruptedException {
         admin.sendMessageTo(TEST_TOPIC_EXCHANGE,"Log.critical", SAMPLE_MESSAGE_TEXT_2.getBytes());
         admin.sendMessageTo(TEST_TOPIC_EXCHANGE,"Alert.critical", SAMPLE_MESSAGE_TEXT_2.getBytes());
         admin.sendMessageTo(TEST_TOPIC_EXCHANGE,"Log.debug.dao", SAMPLE_MESSAGE_TEXT_2.getBytes());

    }

    @Test
    public void consumeMessageFromTopicQueues() throws IOException, InterruptedException {
        String message = admin.consumeMessageFrom(TEST_TOPIC_QUEUE);

        assertEquals(SAMPLE_MESSAGE_TEXT, message);

        message = admin.consumeMessageFrom(TEST_TOPIC_QUEUE_2);

        assertEquals(SAMPLE_MESSAGE_TEXT, message);

        message = admin.consumeMessageFrom(TEST_TOPIC_QUEUE_3);

        assertEquals(SAMPLE_MESSAGE_TEXT, message);
    }




    @Test
    public void create_NonPersistent_Queue() throws IOException {
        Channel channel = admin.createQueueWhichDiesAfterServerRestart("NonDurable  Queue");
        assertNotNull(channel);
    }

    @Test
    public void create_Persistent_Queue() throws IOException {
        Channel channel = admin.createDurableQueue("Durable  Queue");
        assertNotNull(channel);
    }

    @Test
    public void NackMethodWithRequeue() throws IOException, InterruptedException {
        String queueName="NACK_QUEUE";
        messageNackFunctionality(true, queueName);
    }

    private void messageNackFunctionality(boolean requeue, String queueName) throws IOException, InterruptedException {
        for(int i =0;i<10;i++) {
            String messageText = SAMPLE_MESSAGE_TEXT ;
            admin.sendMessage(queueName, messageText.getBytes());
            String consumedMessage = admin.consumeMessageWithNackConsumer(queueName, requeue);
            assertEquals(messageText, consumedMessage);
        }
    }

    @Test
    public void NackMethodWithRequeueDisabled() throws IOException, InterruptedException {
        String queueName="NACK_QUEUE";
        messageNackFunctionality(false, queueName);
    }

    @Test
    public void bindQueueToExchange_RoutingKey() throws IOException, InterruptedException {
        admin.createAndBindExchangeToQueue(TEST_EXCHANGE_1,TEST_QUEUE,"direct","log.*");
        admin.createAndBindExchangeToQueue(TEST_EXCHANGE_1,TEST_QUEUE_3,"direct","*.critical");
        admin.createAndBindExchangeToQueue(TEST_EXCHANGE_1,"NEW Queue","direct","alert.critical");

        admin.sendMessageTo(TEST_EXCHANGE_1, SAMPLE_MESSAGE_TEXT_2.getBytes());

        String message = admin.consumeMessageFrom(TEST_QUEUE);

        assertEquals(SAMPLE_MESSAGE_TEXT_2, message);
    }


    @Test
    public void deleteExchange() throws IOException {
        admin.createAndBindExchangeToQueue("ERROR","ErrorQueue","direct","error");
        admin.deleteExchange("ERROR");
    }


    @Test
    public void createDeadLetterExchange() throws IOException, InterruptedException {
        admin.createAndBindExchangeToQueue("Dead-Letter-Exchange","Dead-Letter-Queue","direct","dead-message");
         Map<String, Object> args = new HashMap<String, Object>();
        args.put("x-dead-letter-exchange", "Dead-Letter-Exchange");
        String queueName ="TEST_DEAD_LETTER_QUEUE";
        admin.createQueue(queueName,args);

        messageNackFunctionality(false, queueName);

    }

    @Test
    public void main() throws IOException {

         admin.sendMessageTo("TEST_FANOUT_EXCHANGE","SAMPLE TEXT ".getBytes());

    }


}
