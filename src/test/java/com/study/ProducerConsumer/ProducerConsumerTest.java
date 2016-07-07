package com.study.ProducerConsumer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static junit.framework.TestCase.assertNotNull;

public class ProducerConsumerTest {
    public static final String TEST_QUEUE = "TestQueue";
    public static final String TEST_QUEUE_2 = "TestQueue_2";
    public static final String SAMPLE_MESSAGE_TEXT = "SampleMessage";
    public static final String SAMPLE_MESSAGE_TEXT_2 = "SampleMessage 2";
    public static final String TEST_EXCHANGE = "TEST_EXCHANGE";
    public static final String TEST_FANOUT_EXCHANGE = "TEST_FANOUT_EXCHANGE";
    private static final java.lang.String TEST_QUEUE_3 = "TestQueue3";
    private static final String TEST_EXCHANGE_1 = "TEST_EXCHANGE_1";
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
    public void createTemporary_AutoDelete_Queue() throws IOException {
        Channel channel = admin.createTempQueue("NonPersistant_AutoDelete Queue");
        assertNotNull(channel);
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
    public void sendAndReceiveMessageToDefaultExchange() throws IOException, InterruptedException {
        admin.sendMessage(TEST_QUEUE, SAMPLE_MESSAGE_TEXT.getBytes());

        String message = admin.consumeMessageFrom(TEST_QUEUE);

        assertEquals(SAMPLE_MESSAGE_TEXT, message);
    }


    @Test
    public void bindQueueToExchange() throws IOException, InterruptedException {
        admin.createAndBindExchangeToQueue(TEST_EXCHANGE,TEST_QUEUE);
        admin.createAndBindExchangeToQueue(TEST_EXCHANGE,TEST_QUEUE_3);
        admin.createAndBindExchangeToQueue(TEST_EXCHANGE,"NEW Queue");

        admin.sendMessageTo(TEST_EXCHANGE, SAMPLE_MESSAGE_TEXT_2.getBytes());

        String message = admin.consumeMessageFrom(TEST_QUEUE);

        assertEquals(SAMPLE_MESSAGE_TEXT_2, message);
    }

    @Test
    public void testFanoutExchange() throws IOException, InterruptedException {
        admin.createAndBindExchangeToQueue(TEST_FANOUT_EXCHANGE,TEST_QUEUE,"fanout");
        admin.createAndBindExchangeToQueue(TEST_FANOUT_EXCHANGE,TEST_QUEUE_2,"fanout");
        admin.createAndBindExchangeToQueue(TEST_FANOUT_EXCHANGE,TEST_QUEUE_3,"fanout");

        admin.sendMessageTo(TEST_FANOUT_EXCHANGE, SAMPLE_MESSAGE_TEXT.getBytes());

        String message = admin.consumeMessageFrom(TEST_QUEUE);

        assertEquals(SAMPLE_MESSAGE_TEXT, message);

        message = admin.consumeMessageFrom(TEST_QUEUE_2);

        assertEquals(SAMPLE_MESSAGE_TEXT, message);

        message = admin.consumeMessageFrom(TEST_QUEUE_3);

        assertEquals(SAMPLE_MESSAGE_TEXT, message);
    }

    @Test
    public void NackMethodWithRequeue() throws IOException, InterruptedException {
        messageNackFunctionality(true);
    }

    private void messageNackFunctionality(boolean requeue) throws IOException, InterruptedException {
        for(int i =0;i<10;i++) {
            String messageText = SAMPLE_MESSAGE_TEXT ;
            admin.sendMessage("NACK_QUEUE", messageText.getBytes());
            String consumedMessage = admin.consumeMessageWithNackConsumer("NACK_QUEUE", requeue);
            assertEquals(messageText, consumedMessage);
        }
    }

    @Test
    public void NackMethodWithRequeueDisabled() throws IOException, InterruptedException {
        messageNackFunctionality(false);
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

}
