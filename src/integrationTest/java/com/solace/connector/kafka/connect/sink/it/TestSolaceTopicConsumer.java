package com.solace.connector.kafka.connect.sink.it;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPErrorResponseException;
import com.solacesystems.jcsmp.JCSMPErrorResponseSubcodeEx;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class TestSolaceTopicConsumer implements AutoCloseable {

    public static BlockingQueue<BytesXMLMessage> solaceReceivedTopicMessages  = new ArrayBlockingQueue<>(10000);

    static Logger logger = LoggerFactory.getLogger(TestSolaceTopicConsumer.class);
    private final JCSMPSession session;
    private XMLMessageConsumer topicSubscriber;

    public TestSolaceTopicConsumer(JCSMPSession session) {
        this.session = session;
    }

    public void start() throws JCSMPException {
        topicSubscriber = session.getMessageConsumer(new XMLMessageListener() {
            @Override
            public void onReceive(BytesXMLMessage msg) {
               logger.info("Message received to topic: " + msg.getDestination());
               solaceReceivedTopicMessages.add(msg);
            }
            @Override
            public void onException(JCSMPException e) {
                logger.error("Consumer received exception", e);
            }
        });
        // Subscribe to all topics starting a common root
        try {
            session.addSubscription(JCSMPFactory.onlyInstance().createTopic(TestConstants.SOL_ROOT_TOPIC + "/>"));
        } catch (JCSMPErrorResponseException e) {
            if (e.getSubcodeEx() != JCSMPErrorResponseSubcodeEx.SUBSCRIPTION_ALREADY_PRESENT) {
                throw e;
            }
        }
        // Also add subscriptions for DynamicDestination record processor testing
        try {
            session.addSubscription(JCSMPFactory.onlyInstance().createTopic("ctrl" + "/>"));
        } catch (JCSMPErrorResponseException e) {
            if (e.getSubcodeEx() != JCSMPErrorResponseSubcodeEx.SUBSCRIPTION_ALREADY_PRESENT) {
                throw e;
            }
        }
        try {
            session.addSubscription(JCSMPFactory.onlyInstance().createTopic("comms" + "/>"));
        } catch (JCSMPErrorResponseException e) {
            if (e.getSubcodeEx() != JCSMPErrorResponseSubcodeEx.SUBSCRIPTION_ALREADY_PRESENT) {
                throw e;
            }
        }
        logger.info("Topic subscriber connected. Awaiting message...");
        topicSubscriber.start();
    }

    @Override
    public void close() {
        topicSubscriber.stop();
    }
}
