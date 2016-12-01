package com.study.ProducerConsumer;


import org.junit.Test;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.io.IOException;
import java.util.Properties;

public class WildFlyMessagingTest {
    @Test
    public void sendMessagesToWildfly10() throws IOException, NamingException {
        String Message = "Hello, World!";
        String connectionFactoryJNDIName = "jms/RemoteConnectionFactory";
        String queueName = "jms/queue/CommonQueue";
        Context namingContext = null;
        JMSContext context = null;
        try {
            Properties env = new Properties();
            env.put(Context.INITIAL_CONTEXT_FACTORY,"org.jboss.naming.remote.client.InitialContextFactory");
            env.put(Context.PROVIDER_URL, "http-remoting://localhost:8470");
            // Below May Not be needed if you have turned off the security for Messaging
            env.put(Context.SECURITY_PRINCIPAL, "guest");
            env.put(Context.SECURITY_CREDENTIALS, "guest");
            namingContext = new InitialContext(env);
            ConnectionFactory connectionFactory = (ConnectionFactory) namingContext.lookup(connectionFactoryJNDIName);
            Destination destination = (Destination) namingContext.lookup(queueName);
            context = connectionFactory.createContext("userName", "password");
            JMSProducer producer = context.createProducer();
            for (int i = 0; i < 10; i++) {
                producer.send(destination, Message+" - "+i);
                System.out.println("Sent Message - "+Message+" - "+i);
            }
            JMSConsumer consumer = context.createConsumer(destination);

            Thread.sleep(2000);
            for (int i = 0; i < 10; i++) {
                String text = consumer.receiveBody(String.class, 5000);
                System.out.println("Received : "+ text);
            }

            System.out.println("Sent and received messages successfully to Artemis Wildfly");
        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            if (namingContext != null) {
                namingContext.close();
            }
            if (context != null) {
                context.close();
            }
        }

    }
}
