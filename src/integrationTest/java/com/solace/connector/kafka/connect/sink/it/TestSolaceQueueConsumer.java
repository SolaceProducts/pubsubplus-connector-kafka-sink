package com.solace.connector.kafka.connect.sink.it;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.XMLMessageListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class TestSolaceQueueConsumer implements AutoCloseable {
    public static BlockingQueue<BytesXMLMessage> solaceReceivedQueueMessages  = new ArrayBlockingQueue<>(10000);

    static Logger logger = LoggerFactory.getLogger(TestSolaceQueueConsumer.class);
    private final JCSMPSession session;
    private FlowReceiver queueConsumer;
    private String queueName;

    public TestSolaceQueueConsumer(JCSMPSession session) {
        this.session = session;
    }

    public void provisionQueue(String queueName) throws JCSMPException {
        provisionQueue(queueName, new EndpointProperties());
    }

    public void provisionQueue(String queueName, EndpointProperties endpointProps) throws JCSMPException {
        setQueueName(queueName);
        final Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);
        // Provision queue in case it doesn't exist, and do not fail if it already exists
        endpointProps.setPermission(EndpointProperties.PERMISSION_CONSUME);
        endpointProps.setAccessType(EndpointProperties.ACCESSTYPE_NONEXCLUSIVE);
        session.provision(queue, endpointProps, JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS);
        logger.info("Ensured Solace queue " + queueName + " exists.");
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public void start() throws JCSMPException {
        final ConsumerFlowProperties flow_prop = new ConsumerFlowProperties();
        flow_prop.setEndpoint(JCSMPFactory.onlyInstance().createQueue(queueName));
        flow_prop.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);
        EndpointProperties endpoint_props = new EndpointProperties();
        endpoint_props.setAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);
        queueConsumer = session.createFlow(new XMLMessageListener() {
            @Override
            public void onReceive(BytesXMLMessage msg) {
                logger.info("Queue message received from {} with user properties: {}", msg.getDestination(),
                        msg.getProperties());
                solaceReceivedQueueMessages.add(msg);
                msg.ackMessage();
            }

            @Override
            public void onException(JCSMPException e) {
                logger.error("Consumer received exception", e);
            }
        }, flow_prop, endpoint_props);

        // Start the consumer
        logger.info("Queue receiver connected. Awaiting message...");
        queueConsumer.start();
    }

    @Override
    public void close() {
        queueConsumer.stop();
    }
}
