package com.solace.connector.kafka.connect.sink;

import com.solace.test.integration.junit.jupiter.extension.PubSubPlusExtension;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.Endpoint;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@ExtendWith(PubSubPlusExtension.class)
public class SolaceProducerHandlerIT {
	private Map<String, String> properties;
	private SolSessionHandler sessionHandler;
	private SolProducerHandler producerHandler;

	private static final Logger logger = LoggerFactory.getLogger(SolaceProducerHandlerIT.class);

	@BeforeEach
	void setUp(JCSMPProperties jcsmpProperties) {
		properties = new HashMap<>();
		properties.put(SolaceSinkConstants.SOL_HOST, jcsmpProperties.getStringProperty(JCSMPProperties.HOST));
		properties.put(SolaceSinkConstants.SOL_VPN_NAME, jcsmpProperties.getStringProperty(JCSMPProperties.VPN_NAME));
		properties.put(SolaceSinkConstants.SOL_USERNAME, jcsmpProperties.getStringProperty(JCSMPProperties.USERNAME));
		properties.put(SolaceSinkConstants.SOL_PASSWORD, jcsmpProperties.getStringProperty(JCSMPProperties.PASSWORD));
	}

	@AfterEach
	void tearDown() {
		if (producerHandler != null) {
			producerHandler.close();
		}

		if (sessionHandler != null) {
			sessionHandler.shutdown();
		}
	}

	@CartesianTest(name = "[{index}] txnQueue={0}, txnTopic={1}, sendToQueue={2}, sendToTopic={3}")
	public void testStaticDestinations(@Values(booleans = {true, false}) boolean txnQueue,
									   @Values(booleans = {true, false}) boolean txnTopic,
									   @Values(booleans = {true, false}) boolean sendToQueue,
									   @Values(booleans = {true, false}) boolean sendToTopic,
									   JCSMPSession session,
									   Queue queue) throws Exception {
		properties.put(SolaceSinkConstants.SOl_QUEUE, queue.getName());
		properties.put(SolaceSinkConstants.SOL_TOPICS, RandomStringUtils.randomAlphanumeric(100));
		properties.put(SolaceSinkConstants.SOl_USE_TRANSACTIONS_FOR_QUEUE, Boolean.toString(txnQueue));
		properties.put(SolaceSinkConstants.SOl_USE_TRANSACTIONS_FOR_TOPICS, Boolean.toString(txnTopic));

		SolaceSinkConnectorConfig config = new SolaceSinkConnectorConfig(properties);
		sessionHandler = new SolSessionHandler(config);
		sessionHandler.configureSession();
		sessionHandler.connectSession();
		producerHandler = new SolProducerHandler(config, sessionHandler, null);

		assertNotNull(producerHandler.producer);
		assertNotNull(producerHandler.queueProducer);
		assertNotNull(producerHandler.topicProducer);
		assertEquals(txnTopic ? producerHandler.transactedProducer : producerHandler.producer, producerHandler.topicProducer);

		if (txnQueue || txnTopic) {
			assertNotNull(sessionHandler.getTxSession());
			assertNotNull(producerHandler.transactedProducer);
			assertNotEquals(producerHandler.producer, producerHandler.transactedProducer);
		} else {
			assertNull(sessionHandler.getTxSession());
			assertNull(producerHandler.transactedProducer);
		}

		if (txnQueue) {
			assertEquals(producerHandler.transactedProducer, producerHandler.queueProducer);
		} else {
			assertNotEquals(producerHandler.transactedProducer, producerHandler.queueProducer);
		}

		if (sendToQueue) {
			sendAndAssert(session, properties.get(SolaceSinkConstants.SOl_QUEUE), Queue.class, queue, txnQueue);
		}

		if (sendToTopic) {
			sendAndAssert(session, properties.get(SolaceSinkConstants.SOL_TOPICS), Topic.class, queue, txnTopic);
		}
	}

	@CartesianTest(name = "[{index}] txnQueue={0}, txnTopic={1}, sendToQueue={2}, sendToTopic={3}")
	public void testLazyInit(@Values(booleans = {true, false}) boolean txnQueue,
							 @Values(booleans = {true, false}) boolean txnTopic,
							 @Values(booleans = {true, false}) boolean sendToQueue,
							 @Values(booleans = {true, false}) boolean sendToTopic,
							 JCSMPSession session,
							 Queue queue) throws Exception {
		properties.put(SolaceSinkConstants.SOl_USE_TRANSACTIONS_FOR_QUEUE, Boolean.toString(txnQueue));
		properties.put(SolaceSinkConstants.SOl_USE_TRANSACTIONS_FOR_TOPICS, Boolean.toString(txnTopic));

		SolaceSinkConnectorConfig config = new SolaceSinkConnectorConfig(properties);
		sessionHandler = new SolSessionHandler(config);
		sessionHandler.configureSession();
		sessionHandler.connectSession();
		producerHandler = new SolProducerHandler(config, sessionHandler, null);

		assertNotNull(producerHandler.producer);
		assertNull(sessionHandler.getTxSession());
		assertNull(producerHandler.transactedProducer);
		assertNull(producerHandler.topicProducer);
		assertNull(producerHandler.queueProducer);

		if (sendToQueue) {
			sendAndAssert(session, queue.getName(), Queue.class, queue, txnQueue);
		}

		if (sendToTopic) {
			sendAndAssert(session, RandomStringUtils.randomAlphanumeric(100), Topic.class, queue, txnTopic);
		}

		if ((sendToQueue && txnQueue) || (sendToTopic && txnTopic)) {
			assertNotNull(sessionHandler.getTxSession());
			assertNotNull(producerHandler.transactedProducer);
			assertNotEquals(producerHandler.producer, producerHandler.transactedProducer);
		} else {
			assertNull(sessionHandler.getTxSession());
			assertNull(producerHandler.transactedProducer);
		}

		if (sendToQueue) {
			assertNotNull(producerHandler.queueProducer);
			if (txnQueue) {
				assertEquals(producerHandler.transactedProducer, producerHandler.queueProducer);
			} else {
				assertNotEquals(producerHandler.transactedProducer, producerHandler.queueProducer);
			}
		} else {
			assertNull(producerHandler.queueProducer);
		}

		if (sendToTopic) {
			assertNotNull(producerHandler.topicProducer);
			assertEquals(txnTopic ? producerHandler.transactedProducer : producerHandler.producer, producerHandler.topicProducer);
		} else {
			assertNull(producerHandler.topicProducer);
		}
	}

	@Test
	public void testAutoFlush(JCSMPSession session, Queue queue) throws Exception {
		List<String> topics = IntStream.range(0, 9)
				.mapToObj(i -> RandomStringUtils.randomAlphanumeric(100))
				.collect(Collectors.toList());
		properties.put(SolaceSinkConstants.SOl_QUEUE, queue.getName());
		properties.put(SolaceSinkConstants.SOL_TOPICS, String.join(",", topics));
		properties.put(SolaceSinkConstants.SOl_USE_TRANSACTIONS_FOR_QUEUE, Boolean.toString(true));
		properties.put(SolaceSinkConstants.SOl_USE_TRANSACTIONS_FOR_TOPICS, Boolean.toString(true));
		properties.put(SolaceSinkConstants.SOL_AUTOFLUSH_SIZE, Integer.toString(2));
		final int expectedNumCommits = (topics.size() + 1) /
				Integer.parseInt(properties.get(SolaceSinkConstants.SOL_AUTOFLUSH_SIZE));

		SolaceSinkConnectorConfig config = new SolaceSinkConnectorConfig(properties);
		sessionHandler = new SolSessionHandler(config);
		sessionHandler.configureSession();
		sessionHandler.connectSession();
		AtomicInteger numCommits = new AtomicInteger();
		producerHandler = new SolProducerHandler(config, sessionHandler, () -> {
			try {
				sessionHandler.getTxSession().commit();
				producerHandler.getTxMsgCount().set(0);
			} catch (JCSMPException e) {
				throw new RuntimeException(e);
			}
			logger.info("Commit #{}", numCommits.incrementAndGet());
		});

		sendAndAssert(session, queue.getName(), Queue.class, queue, true, false, false);
		for (String topic : topics) {
			sendAndAssert(session, topic, Topic.class, queue, true, false, false);
		}

		assertEquals(expectedNumCommits, numCommits.get());
	}

	private void sendAndAssert(JCSMPSession session, String destination, Class<? extends Destination> destinationType,
							   Endpoint receiveEndpoint, boolean isTransacted) throws JCSMPException {
		sendAndAssert(session, destination, destinationType, receiveEndpoint, isTransacted, isTransacted, true);
	}

	private void sendAndAssert(JCSMPSession session, String destination, Class<? extends Destination> destinationType,
							   Endpoint receiveEndpoint, boolean isTransacted, boolean doCommit, boolean doConsume)
			throws JCSMPException {
		Destination sendDestination;
		if (destinationType.isAssignableFrom(Queue.class)) {
			sendDestination = JCSMPFactory.onlyInstance().createQueue(destination);
		} else {
			Topic topic = JCSMPFactory.onlyInstance().createTopic(destination);
			session.addSubscription(receiveEndpoint, topic, JCSMPSession.WAIT_FOR_CONFIRM);
			sendDestination = topic;
		}

		TextMessage message = Mockito.spy(JCSMPFactory.onlyInstance().createMessage(TextMessage.class));
		message.setText(RandomStringUtils.randomAlphanumeric(100));
		producerHandler.send(message, sendDestination);
		if (doCommit) {
			sessionHandler.getTxSession().commit();
		}

		if (doConsume) {
			ConsumerFlowProperties consumerFlowProperties = new ConsumerFlowProperties();
			consumerFlowProperties.setEndpoint(receiveEndpoint);
			consumerFlowProperties.setStartState(true);
			FlowReceiver flowReceiver = session.createFlow(null, consumerFlowProperties);
			BytesXMLMessage receivedMessage;
			try {
				receivedMessage = flowReceiver.receive(Math.toIntExact(TimeUnit.SECONDS.toMillis(30)));
			} finally {
				flowReceiver.close();
			}

			assertInstanceOf(TextMessage.class, receivedMessage);
			assertEquals(message.getText(), ((TextMessage) receivedMessage).getText());
			assertEquals(destination, receivedMessage.getDestination().getName());
		}

		Mockito.verify(message).setDeliveryMode(destinationType.isAssignableFrom(Queue.class) || isTransacted ?
				DeliveryMode.PERSISTENT : DeliveryMode.DIRECT);
	}
}
