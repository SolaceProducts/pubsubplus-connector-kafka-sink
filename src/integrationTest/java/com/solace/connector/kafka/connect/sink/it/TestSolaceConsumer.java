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
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class TestSolaceConsumer {

    // Queue to communicate received messages
    public static BlockingQueue<BytesXMLMessage> solaceReceivedTopicMessages  = new ArrayBlockingQueue<>(10000);
    public static BlockingQueue<BytesXMLMessage> solaceReceivedQueueMessages  = new ArrayBlockingQueue<>(10000);

    static Logger logger = LoggerFactory.getLogger(SinkConnectorIT.class.getName());
    private JCSMPProperties properties;
    private JCSMPSession session;
    private XMLMessageConsumer topicSubscriber;
    private FlowReceiver queueConsumer;
    private String queueName;

    public void initialize() {
        TestConfigProperties configProps = new TestConfigProperties();
        properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, "tcp://" + configProps.getProperty("sol.host") + ":55555");     // host:port
        properties.setProperty(JCSMPProperties.USERNAME, configProps.getProperty("sol.username")); // client-username
        properties.setProperty(JCSMPProperties.VPN_NAME,  configProps.getProperty("sol.vpn_name")); // message-vpn
        properties.setProperty(JCSMPProperties.PASSWORD, configProps.getProperty("sol.password")); // client-password
        try {
            session =  JCSMPFactory.onlyInstance().createSession(properties);
            session.connect();
        } catch (JCSMPException e1) {
            e1.printStackTrace();
        }
    }

    public void provisionQueue(String queueName) throws JCSMPException {
        provisionQueue(queueName, new EndpointProperties());
    }

    public void provisionQueue(String queueName, EndpointProperties endpointProps) throws JCSMPException {
        this.queueName = queueName;
        final Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);
        // Provision queue in case it doesn't exist, and do not fail if it already exists
        endpointProps.setPermission(EndpointProperties.PERMISSION_CONSUME);
        endpointProps.setAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);
        session.provision(queue, endpointProps, JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS);
        logger.info("Ensured Solace queue " + queueName + " exists.");
    }

    public void start() throws JCSMPException {

        // Create and start topic subscriber

        topicSubscriber = session.getMessageConsumer(new XMLMessageListener() {
            @Override
            public void onReceive(BytesXMLMessage msg) {
               logger.info("Message received to topic: " + msg.getDestination());
               solaceReceivedTopicMessages.add(msg);
            }
            @Override
            public void onException(JCSMPException e) {
                System.out.printf("Consumer received exception: %s%n",e);
            }
        });
        // Subscribe to all topics starting a common root
        session.addSubscription(JCSMPFactory.onlyInstance().createTopic(TestConstants.SOL_ROOT_TOPIC + "/>"));
        // Also add subscriptions for DynamicDestination record processor testing
        session.addSubscription(JCSMPFactory.onlyInstance().createTopic("ctrl" + "/>"));
        session.addSubscription(JCSMPFactory.onlyInstance().createTopic("comms" + "/>"));
        logger.info("Topic subscriber connected. Awaiting message...");
        topicSubscriber.start();

        // Create and start queue consumer
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
                System.out.printf("Consumer received exception: %s%n", e);
            }
        }, flow_prop, endpoint_props);

        // Start the consumer
        logger.info("Queue receiver connected. Awaiting message...");
        queueConsumer.start();
    }

    public void stop() {
        queueConsumer.stop();
        topicSubscriber.stop();
        session.closeSession();
    }

    public JCSMPSession getSession() {
        return session;
    }

    public JCSMPProperties getProperties() {
        return properties;
    }
}
