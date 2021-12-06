package com.solace.connector.kafka.connect.sink.it;

import com.solace.connector.kafka.connect.sink.SolRecordProcessorIF;
import com.solace.connector.kafka.connect.sink.SolSessionEventCallbackHandler;
import com.solace.connector.kafka.connect.sink.SolaceSinkConstants;
import com.solace.connector.kafka.connect.sink.SolaceSinkSender;
import com.solace.connector.kafka.connect.sink.SolaceSinkTask;
import com.solace.connector.kafka.connect.sink.it.util.extensions.NetworkPubSubPlusExtension;
import com.solace.connector.kafka.connect.sink.recordprocessor.SolDynamicDestinationRecordProcessor;
import com.solace.connector.kafka.connect.sink.recordprocessor.SolSimpleRecordProcessor;
import com.solace.test.integration.junit.jupiter.extension.ExecutorServiceExtension;
import com.solace.test.integration.junit.jupiter.extension.ExecutorServiceExtension.ExecSvc;
import com.solace.test.integration.junit.jupiter.extension.LogCaptorExtension;
import com.solace.test.integration.junit.jupiter.extension.LogCaptorExtension.LogCaptor;
import com.solace.test.integration.junit.jupiter.extension.PubSubPlusExtension.JCSMPProxy;
import com.solace.test.integration.junit.jupiter.extension.PubSubPlusExtension.ToxiproxyContext;
import com.solace.test.integration.semp.v2.SempV2Api;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnClientProfile;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnClientUsername;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnQueue;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnQueueSubscription;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ClosedFacilityException;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.SDTException;
import com.solacesystems.jcsmp.SessionEvent;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessage;
import com.solacesystems.jcsmp.transaction.RollbackException;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import eu.rekawek.toxiproxy.model.toxic.Latency;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

@ExtendWith(ExecutorServiceExtension.class)
@ExtendWith(LogCaptorExtension.class)
@ExtendWith(NetworkPubSubPlusExtension.class)
public class SolaceSinkTaskIT {
	private SolaceSinkTask solaceSinkTask;
	private Map<String, String> connectorProperties;
	private String clientProfileName;
	private String clientUsernameName;

	private static final Logger logger = LoggerFactory.getLogger(SolaceSinkTask.class);

	@BeforeEach
	void setUp(JCSMPProperties jcsmpProperties, SempV2Api sempV2Api) throws Exception {
		solaceSinkTask = new SolaceSinkTask();
		String msgVpnName = jcsmpProperties.getStringProperty(JCSMPProperties.VPN_NAME);

		clientProfileName = sempV2Api.config().createMsgVpnClientProfile(msgVpnName, new ConfigMsgVpnClientProfile()
						.allowGuaranteedMsgSendEnabled(true)
						.allowGuaranteedMsgReceiveEnabled(true)
						.allowTransactedSessionsEnabled(true)
						.clientProfileName(RandomStringUtils.randomAlphanumeric(30)), null)
				.getData()
				.getClientProfileName();
		logger.info("Created client profile {}", clientProfileName);

		ConfigMsgVpnClientUsername clientUsername = sempV2Api.config().createMsgVpnClientUsername(msgVpnName,
						new ConfigMsgVpnClientUsername()
								.clientUsername(RandomStringUtils.randomAlphanumeric(30))
								.clientProfileName(clientProfileName)
								.enabled(true), null)
				.getData();
		clientUsernameName = clientUsername.getClientUsername();
		logger.info("Created client username {}", clientUsernameName);

		connectorProperties = new HashMap<>();
		connectorProperties.put(SolaceSinkConstants.SOL_RECORD_PROCESSOR, TestConstants.CONN_MSGPROC_CLASS);
		connectorProperties.put(SolaceSinkConstants.SOL_HOST, jcsmpProperties.getStringProperty(JCSMPProperties.HOST));
		connectorProperties.put(SolaceSinkConstants.SOL_VPN_NAME, msgVpnName);
		connectorProperties.put(SolaceSinkConstants.SOL_USERNAME, clientUsername.getClientUsername());
		Optional.ofNullable(clientUsername.getPassword())
				.ifPresent(p -> connectorProperties.put(SolaceSinkConstants.SOL_PASSWORD, p));
	}

	@AfterEach
	void tearDown(JCSMPProperties jcsmpProperties, SempV2Api sempV2Api) throws Exception {
		String msgVpnName = jcsmpProperties.getStringProperty(JCSMPProperties.VPN_NAME);

		solaceSinkTask.stop();

		if (clientUsernameName != null) {
			logger.info("Deleting client username {}", clientUsernameName);
			sempV2Api.config().deleteMsgVpnClientUsername(msgVpnName, clientUsernameName);
		}

		if (clientProfileName != null) {
			logger.info("Deleting client profile {}", clientProfileName);
			sempV2Api.config().deleteMsgVpnClientProfile(msgVpnName, clientProfileName);
		}
	}

	@Test
	public void testNoProvidedMessageProcessor() {
		connectorProperties.remove(SolaceSinkConstants.SOL_RECORD_PROCESSOR);
		ConnectException thrown = assertThrows(ConnectException.class, () -> solaceSinkTask.start(connectorProperties));
		assertThat(thrown.getMessage(), containsString("Failed to setup sender to PubSub+"));
		assertThat(thrown.getCause(), instanceOf(KafkaException.class));
		assertThat(thrown.getCause().getMessage(), containsString(
				"Could not find a public no-argument constructor for " + SolRecordProcessorIF.class.getName()));
	}

	@ParameterizedTest(name = "[{index}] transacted={0}")
	@ValueSource(booleans = { true, false })
	public void testFailCreateQueueProducer(boolean transacted, SempV2Api sempV2Api, Queue queue) throws Exception {
		connectorProperties.put(SolaceSinkConstants.SOl_QUEUE, queue.getName());
		connectorProperties.put(SolaceSinkConstants.SOl_USE_TRANSACTIONS_FOR_QUEUE, Boolean.toString(transacted));

		sempV2Api.config().updateMsgVpnClientProfile(connectorProperties.get(SolaceSinkConstants.SOL_VPN_NAME),
				clientProfileName,
				new ConfigMsgVpnClientProfile().allowGuaranteedMsgSendEnabled(false), null);

		ConnectException thrown = assertThrows(ConnectException.class, () -> solaceSinkTask.start(connectorProperties));
		assertThat(thrown.getMessage(), containsString("Failed to setup sender to PubSub+"));
		assertThat(thrown.getCause(), instanceOf(JCSMPException.class));
		assertThat(thrown.getCause().getMessage(), containsString("Router does not support guaranteed publisher flows"));
	}

	@Test
	public void testFailTransactedSessionCreation(SempV2Api sempV2Api, Queue queue) throws Exception {
		connectorProperties.put(SolaceSinkConstants.SOl_QUEUE, queue.getName());
		connectorProperties.put(SolaceSinkConstants.SOl_USE_TRANSACTIONS_FOR_QUEUE, "true");

		sempV2Api.config().updateMsgVpnClientProfile(connectorProperties.get(SolaceSinkConstants.SOL_VPN_NAME),
				clientProfileName,
				new ConfigMsgVpnClientProfile().allowTransactedSessionsEnabled(false), null);

		ConnectException thrown = assertThrows(ConnectException.class, () -> solaceSinkTask.start(connectorProperties));
		assertThat(thrown.getCause(), instanceOf(JCSMPException.class));
		assertThat(thrown.getCause().getMessage(), containsString("Router does not support transacted sessions"));
	}

	@ParameterizedTest
	@ValueSource(classes = {Queue.class, Topic.class})
	public void testSendThrowsJCSMPException(Class<Destination> destinationType, Queue queue) {
		if (destinationType.isAssignableFrom(Queue.class)) {
			connectorProperties.put(SolaceSinkConstants.SOl_QUEUE, queue.getName());
		} else {
			connectorProperties.put(SolaceSinkConstants.SOL_TOPICS, RandomStringUtils.randomAlphanumeric(100));
		}

		solaceSinkTask.start(connectorProperties);

		SinkRecord sinkRecord = new SinkRecord(RandomStringUtils.randomAlphanumeric(100), 0,
				Schema.STRING_SCHEMA, RandomStringUtils.randomAlphanumeric(100),
				Schema.BYTES_SCHEMA, RandomUtils.nextBytes(10), 0);

		solaceSinkTask.stop();
		ConnectException thrown = assertThrows(ConnectException.class, () -> solaceSinkTask.put(
				Collections.singleton(sinkRecord)));
		assertThat(thrown, instanceOf(RetriableException.class));
		assertThat(thrown.getMessage(), containsString("Received exception while sending message to " +
				(destinationType.isAssignableFrom(Queue.class) ? "queue" : "topic")));
		assertThat(thrown.getCause(), instanceOf(ClosedFacilityException.class));
	}

	@ParameterizedTest(name = "[{index}] destinationType={0}")
	@ValueSource(classes = {Queue.class, Topic.class})
	public void testDynamicSendThrowsJCSMPException(Class<Destination> destinationType, Queue queue) {
		connectorProperties.put(SolaceSinkConstants.SOL_DYNAMIC_DESTINATION, Boolean.toString(true));
		connectorProperties.put(SolaceSinkConstants.SOL_RECORD_PROCESSOR, DynamicDestinationTypeRecordProcessor.class
				.getName());
		solaceSinkTask.start(connectorProperties);

		SinkRecord sinkRecord = new SinkRecord(RandomStringUtils.randomAlphanumeric(100), 0,
				Schema.STRING_SCHEMA, RandomStringUtils.randomAlphanumeric(100),
				Schema.BYTES_SCHEMA, RandomUtils.nextBytes(10), 0);

		String dynamicDestinationName = destinationType.isAssignableFrom(Queue.class) ? queue.getName() :
				RandomStringUtils.randomAlphanumeric(100);
		sinkRecord.headers()
				.addString(DynamicDestinationTypeRecordProcessor.HEADER_DYNAMIC_DESTINATION, dynamicDestinationName)
				.addString(DynamicDestinationTypeRecordProcessor.HEADER_DYNAMIC_DESTINATION_TYPE, destinationType.getName());

		solaceSinkTask.stop();
		ConnectException thrown = assertThrows(ConnectException.class, () -> solaceSinkTask.put(
				Collections.singleton(sinkRecord)));
		assertThat(thrown, instanceOf(RetriableException.class));
		assertThat(thrown.getMessage(), containsString("Received exception while sending message to topic"));
		assertThat(thrown.getCause(), instanceOf(ClosedFacilityException.class));
	}

	@ParameterizedTest(name = "[{index}] ignoreRecordProcessorError={0}")
	@ValueSource(booleans = { true, false })
	public void testInvalidDynamicDestination(boolean ignoreRecordProcessorError,
											  @ExecSvc ExecutorService executorService,
											  @LogCaptor(SolaceSinkSender.class) BufferedReader logReader) throws Exception {
		connectorProperties.put(SolaceSinkConstants.SOL_RECORD_PROCESSOR, BadSolDynamicDestinationRecordProcessor.class.getName());
		connectorProperties.put(SolaceSinkConstants.SOL_RECORD_PROCESSOR_IGNORE_ERROR, Boolean.toString(ignoreRecordProcessorError));
		connectorProperties.put(SolaceSinkConstants.SOL_DYNAMIC_DESTINATION, Boolean.toString(true));
		solaceSinkTask.start(connectorProperties);

		Set<SinkRecord> records = Collections.singleton(new SinkRecord(RandomStringUtils.randomAlphanumeric(100), 0,
				Schema.STRING_SCHEMA, RandomStringUtils.randomAlphanumeric(100),
				Schema.BYTES_SCHEMA, String.format("%s %s", RandomStringUtils.randomAlphanumeric(4),
				RandomStringUtils.randomAlphanumeric(100)).getBytes(StandardCharsets.UTF_8), 0));

		if (ignoreRecordProcessorError) {
			Future<?> future = executorService.submit(() -> {
				String logLine;
				do {
					try {
						logLine = logReader.readLine();
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				} while (!logLine.contains("Received exception retrieving Dynamic Destination"));
			});
			solaceSinkTask.put(records);
			future.get(30, TimeUnit.SECONDS);
		} else {
			ConnectException thrown = assertThrows(ConnectException.class, () -> solaceSinkTask.put(records));
			assertThat(thrown.getMessage(), containsString("Received exception retrieving Dynamic Destination"));
			assertThat(thrown.getCause(), instanceOf(SDTException.class));
			assertThat(thrown.getCause().getMessage(), containsString("No conversion from String to Destination"));
		}
	}

	@ParameterizedTest(name = "[{index}] ignoreRecordProcessorError={0}")
	@ValueSource(booleans = { true, false })
	public void testRecordProcessorError(boolean ignoreRecordProcessorError,
										 @ExecSvc ExecutorService executorService,
										 @LogCaptor(SolaceSinkSender.class) BufferedReader logReader) throws Exception {
		connectorProperties.put(SolaceSinkConstants.SOL_RECORD_PROCESSOR, BadRecordProcessor.class.getName());
		connectorProperties.put(SolaceSinkConstants.SOL_RECORD_PROCESSOR_IGNORE_ERROR, Boolean.toString(ignoreRecordProcessorError));
		solaceSinkTask.start(connectorProperties);

		Set<SinkRecord> records = Collections.singleton(new SinkRecord(RandomStringUtils.randomAlphanumeric(100), 0,
				Schema.STRING_SCHEMA, RandomStringUtils.randomAlphanumeric(100),
				Schema.BYTES_SCHEMA, RandomUtils.nextBytes(10), 0));

		if (ignoreRecordProcessorError) {
			Future<?> future = executorService.submit(() -> {
				String logLine;
				do {
					try {
						logLine = logReader.readLine();
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				} while (!logLine.contains("Encountered exception in record processing"));
			});
			solaceSinkTask.put(records);
			future.get(30, TimeUnit.SECONDS);
		} else {
			ConnectException thrown = assertThrows(ConnectException.class, () -> solaceSinkTask.put(records));
			assertThat(thrown.getMessage(), containsString("Encountered exception in record processing"));
			assertEquals(BadRecordProcessor.TEST_EXCEPTION, thrown.getCause());
		}
	}

	@ParameterizedTest(name = "[{index}] autoFlush={0}")
	@ValueSource(booleans = {false, true})
	public void testCommitRollback(boolean autoFlush, SempV2Api sempV2Api, Queue queue) throws Exception {
		connectorProperties.put(SolaceSinkConstants.SOl_QUEUE, queue.getName());
		connectorProperties.put(SolaceSinkConstants.SOL_TOPICS, RandomStringUtils.randomAlphanumeric(100));
		connectorProperties.put(SolaceSinkConstants.SOl_USE_TRANSACTIONS_FOR_QUEUE, Boolean.toString(true));
		connectorProperties.put(SolaceSinkConstants.SOl_USE_TRANSACTIONS_FOR_TOPICS, Boolean.toString(true));
		if (autoFlush) {
			connectorProperties.put(SolaceSinkConstants.SOL_AUTOFLUSH_SIZE, Integer.toString(2));
		}

		String vpnName = connectorProperties.get(SolaceSinkConstants.SOL_VPN_NAME);
		sempV2Api.config().createMsgVpnQueueSubscription(vpnName, queue.getName(), new ConfigMsgVpnQueueSubscription()
				.subscriptionTopic(connectorProperties.get(SolaceSinkConstants.SOL_TOPICS)), null);
		sempV2Api.config().updateMsgVpnQueue(vpnName, queue.getName(), new ConfigMsgVpnQueue().maxMsgSize(1), null);

		assertTimeoutPreemptively(Duration.ofSeconds(20), () -> {
			while (sempV2Api.monitor().getMsgVpnQueue(vpnName, queue.getName(), null).getData()
					.getMaxMsgSize() != 1) {
				logger.info("Waiting for queue {} to have max message size of 1", queue.getName());
				Thread.sleep(100);
			}
		});

		solaceSinkTask.start(connectorProperties);

		SinkRecord sinkRecord = new SinkRecord(RandomStringUtils.randomAlphanumeric(100), 0,
				Schema.STRING_SCHEMA, RandomStringUtils.randomAlphanumeric(100),
				Schema.BYTES_SCHEMA, RandomUtils.nextBytes(10), 0);

		ConnectException thrown;
		if (autoFlush) {
			thrown = assertThrows(RetriableException.class, () -> solaceSinkTask.put(Collections.singleton(sinkRecord)));
		} else {
			Map<TopicPartition, OffsetAndMetadata> currentOffsets = Collections.singletonMap(
					new TopicPartition(sinkRecord.topic(), sinkRecord.kafkaPartition()),
					new OffsetAndMetadata(sinkRecord.kafkaOffset()));
			solaceSinkTask.put(Collections.singleton(sinkRecord));
			thrown = assertThrows(ConnectException.class, () -> solaceSinkTask.flush(currentOffsets));
		}

		assertThat(thrown.getMessage(), containsString("Error in committing transaction"));
		assertThat(thrown.getCause(), instanceOf(RollbackException.class));
		assertThat(thrown.getCause().getMessage(), containsString("Document Is Too Large"));
		assertEquals(2, sempV2Api.monitor().getMsgVpnQueue(vpnName, queue.getName(), null)
				.getData().getMaxMsgSizeExceededDiscardedMsgCount());
	}

	@CartesianTest(name = "[{index}] destinationType={0}, autoFlush={1}")
	public void testDynamicDestinationCommitRollback(
			@Values(classes = {Queue.class, Topic.class}) Class<Destination> destinationType,
			@Values(booleans = {false, true}) boolean autoFlush,
			SempV2Api sempV2Api,
			Queue queue) throws Exception {
		connectorProperties.put(SolaceSinkConstants.SOl_USE_TRANSACTIONS_FOR_QUEUE, Boolean.toString(true));
		connectorProperties.put(SolaceSinkConstants.SOl_USE_TRANSACTIONS_FOR_TOPICS, Boolean.toString(true));
		connectorProperties.put(SolaceSinkConstants.SOL_DYNAMIC_DESTINATION, Boolean.toString(true));
		connectorProperties.put(SolaceSinkConstants.SOL_RECORD_PROCESSOR, DynamicDestinationTypeRecordProcessor.class
				.getName());

		if (autoFlush) {
			connectorProperties.put(SolaceSinkConstants.SOL_AUTOFLUSH_SIZE, Integer.toString(1));
		}

		String topicName = RandomStringUtils.randomAlphanumeric(100);
		String vpnName = connectorProperties.get(SolaceSinkConstants.SOL_VPN_NAME);

		if (destinationType.isAssignableFrom(Topic.class)) {
			sempV2Api.config().createMsgVpnQueueSubscription(vpnName, queue.getName(),
					new ConfigMsgVpnQueueSubscription().subscriptionTopic(topicName), null);
		}

		sempV2Api.config().updateMsgVpnQueue(vpnName, queue.getName(), new ConfigMsgVpnQueue().maxMsgSize(1), null);
		assertTimeoutPreemptively(Duration.ofSeconds(20), () -> {
			while (sempV2Api.monitor().getMsgVpnQueue(vpnName, queue.getName(), null).getData()
					.getMaxMsgSize() != 1) {
				logger.info("Waiting for queue {} to have max message size of 1", queue.getName());
				Thread.sleep(100);
			}
		});

		solaceSinkTask.start(connectorProperties);

		SinkRecord sinkRecord = new SinkRecord(RandomStringUtils.randomAlphanumeric(100), 0,
				Schema.STRING_SCHEMA, RandomStringUtils.randomAlphanumeric(100),
				Schema.BYTES_SCHEMA, RandomUtils.nextBytes(10), 0);

		String dynamicDestinationName = destinationType.isAssignableFrom(Queue.class) ? queue.getName() : topicName;
		sinkRecord.headers()
				.addString(DynamicDestinationTypeRecordProcessor.HEADER_DYNAMIC_DESTINATION, dynamicDestinationName)
				.addString(DynamicDestinationTypeRecordProcessor.HEADER_DYNAMIC_DESTINATION_TYPE, destinationType.getName());

		ConnectException thrown;
		if (autoFlush) {
			thrown = assertThrows(RetriableException.class, () -> solaceSinkTask.put(Collections.singleton(sinkRecord)));
		} else {
			Map<TopicPartition, OffsetAndMetadata> currentOffsets = Collections.singletonMap(
					new TopicPartition(sinkRecord.topic(), sinkRecord.kafkaPartition()),
					new OffsetAndMetadata(sinkRecord.kafkaOffset()));
			solaceSinkTask.put(Collections.singleton(sinkRecord));
			thrown = assertThrows(ConnectException.class, () -> solaceSinkTask.flush(currentOffsets));
		}

		assertThat(thrown.getMessage(), containsString("Error in committing transaction"));
		assertThat(thrown.getCause(), instanceOf(RollbackException.class));
		assertThat(thrown.getCause().getMessage(), containsString("Document Is Too Large"));
		assertEquals(1, sempV2Api.monitor().getMsgVpnQueue(vpnName, queue.getName(), null)
				.getData().getMaxMsgSizeExceededDiscardedMsgCount());
	}

	@ParameterizedTest(name = "[{index}] autoFlush={0}")
	@ValueSource(booleans = {false, true})
	public void testLongCommit(boolean autoFlush,
							   @JCSMPProxy JCSMPSession jcsmpSession,
							   SempV2Api sempV2Api,
							   Queue queue,
							   @JCSMPProxy ToxiproxyContext jcsmpProxyContext,
							   @ExecSvc ExecutorService executorService,
							   @LogCaptor(SolSessionEventCallbackHandler.class) BufferedReader logReader)
			throws Exception {
		connectorProperties.put(SolaceSinkConstants.SOL_HOST, (String) jcsmpSession.getProperty(JCSMPProperties.HOST));
		connectorProperties.put(SolaceSinkConstants.SOl_QUEUE, queue.getName());
		connectorProperties.put(SolaceSinkConstants.SOL_TOPICS, RandomStringUtils.randomAlphanumeric(100));
		connectorProperties.put(SolaceSinkConstants.SOl_USE_TRANSACTIONS_FOR_QUEUE, Boolean.toString(true));
		connectorProperties.put(SolaceSinkConstants.SOl_USE_TRANSACTIONS_FOR_TOPICS, Boolean.toString(true));
		connectorProperties.put(SolaceSinkConstants.SOL_CHANNEL_PROPERTY_reconnectRetries, Integer.toString(-1));

		if (autoFlush) {
			connectorProperties.put(SolaceSinkConstants.SOL_AUTOFLUSH_SIZE, Integer.toString(2));
		}

		String vpnName = connectorProperties.get(SolaceSinkConstants.SOL_VPN_NAME);
		sempV2Api.config().createMsgVpnQueueSubscription(vpnName, queue.getName(), new ConfigMsgVpnQueueSubscription()
				.subscriptionTopic(connectorProperties.get(SolaceSinkConstants.SOL_TOPICS)), null);

		solaceSinkTask.start(connectorProperties);

		SinkRecord sinkRecord = new SinkRecord(RandomStringUtils.randomAlphanumeric(100), 0,
				Schema.STRING_SCHEMA, RandomStringUtils.randomAlphanumeric(100),
				Schema.BYTES_SCHEMA, RandomUtils.nextBytes(10), 0);
		Map<TopicPartition, OffsetAndMetadata> currentOffsets = Collections.singletonMap(
				new TopicPartition(sinkRecord.topic(), sinkRecord.kafkaPartition()),
				new OffsetAndMetadata(sinkRecord.kafkaOffset()));

		logger.info("Cutting JCSMP upstream");
		Latency lag = jcsmpProxyContext.getProxy().toxics()
				.latency("lag", ToxicDirection.UPSTREAM, TimeUnit.HOURS.toMillis(1));

		Future<?> future = executorService.submit(() -> {
			String logLine;
			do {
				try {
					logLine = logReader.readLine();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			} while (!logLine.contains("Received Session Event " + SessionEvent.RECONNECTING));

			try {
				Thread.sleep(TimeUnit.SECONDS.toMillis(5));
			} catch (InterruptedException ignored) {}

			try {
				logger.info("Restoring JCSMP upstream");
				lag.remove();
				logger.info("JCSMP upstream restored");
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		});

		assertTimeoutPreemptively(Duration.ofMinutes(5), () -> {
			solaceSinkTask.put(Collections.singleton(sinkRecord));
			solaceSinkTask.flush(currentOffsets);
		});
		future.get(30, TimeUnit.SECONDS);

		List<Destination> receivedDestinations = new ArrayList<>();
		ConsumerFlowProperties consumerFlowProperties = new ConsumerFlowProperties();
		consumerFlowProperties.setEndpoint(queue);
		consumerFlowProperties.setStartState(true);
		FlowReceiver flow = jcsmpSession.createFlow(null, consumerFlowProperties);
		try {
			assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
				while (receivedDestinations.size() < 2) {
					logger.info("Receiving messages");
					Optional.ofNullable(flow.receive())
							.map(XMLMessage::getDestination)
							.ifPresent(receivedDestinations::add);
				}
			});
		} finally {
			flow.close();
		}

		assertThat(receivedDestinations, hasItems(queue,
				JCSMPFactory.onlyInstance().createTopic(connectorProperties.get(SolaceSinkConstants.SOL_TOPICS))));
	}

	@CartesianTest(name = "[{index}] destinationType={0}, autoFlush={1}")
	public void testDynamicDestinationLongCommit(
			@Values(classes = {Queue.class, Topic.class}) Class<Destination> destinationType,
			@Values(booleans = {false, true}) boolean autoFlush,
			@JCSMPProxy JCSMPSession jcsmpSession,
			SempV2Api sempV2Api,
			Queue queue,
			@JCSMPProxy ToxiproxyContext jcsmpProxyContext,
			@ExecSvc ExecutorService executorService,
			@LogCaptor(SolSessionEventCallbackHandler.class) BufferedReader logReader) throws Exception {
		connectorProperties.put(SolaceSinkConstants.SOL_HOST, (String) jcsmpSession.getProperty(JCSMPProperties.HOST));
		connectorProperties.put(SolaceSinkConstants.SOl_USE_TRANSACTIONS_FOR_QUEUE, Boolean.toString(true));
		connectorProperties.put(SolaceSinkConstants.SOl_USE_TRANSACTIONS_FOR_TOPICS, Boolean.toString(true));
		connectorProperties.put(SolaceSinkConstants.SOL_CHANNEL_PROPERTY_reconnectRetries, Integer.toString(-1));
		connectorProperties.put(SolaceSinkConstants.SOL_DYNAMIC_DESTINATION, Boolean.toString(true));
		connectorProperties.put(SolaceSinkConstants.SOL_RECORD_PROCESSOR, DynamicDestinationTypeRecordProcessor.class
				.getName());

		// Force transacted session to be created during connector-start.
		connectorProperties.put(SolaceSinkConstants.SOL_TOPICS, RandomStringUtils.randomAlphanumeric(100));

		if (autoFlush) {
			connectorProperties.put(SolaceSinkConstants.SOL_AUTOFLUSH_SIZE, Integer.toString(1));
		}

		String topicName = RandomStringUtils.randomAlphanumeric(100);
		if (destinationType.isAssignableFrom(Topic.class)) {
			sempV2Api.config().createMsgVpnQueueSubscription(connectorProperties.get(SolaceSinkConstants.SOL_VPN_NAME),
					queue.getName(), new ConfigMsgVpnQueueSubscription()
							.subscriptionTopic(topicName), null);
		}

		solaceSinkTask.start(connectorProperties);

		SinkRecord sinkRecord = new SinkRecord(RandomStringUtils.randomAlphanumeric(100), 0,
				Schema.STRING_SCHEMA, RandomStringUtils.randomAlphanumeric(100),
				Schema.BYTES_SCHEMA, RandomUtils.nextBytes(10), 0);

		String dynamicDestinationName = destinationType.isAssignableFrom(Queue.class) ? queue.getName() : topicName;
		sinkRecord.headers()
				.addString(DynamicDestinationTypeRecordProcessor.HEADER_DYNAMIC_DESTINATION, dynamicDestinationName)
				.addString(DynamicDestinationTypeRecordProcessor.HEADER_DYNAMIC_DESTINATION_TYPE, destinationType.getName());

		Map<TopicPartition, OffsetAndMetadata> currentOffsets = Collections.singletonMap(
				new TopicPartition(sinkRecord.topic(), sinkRecord.kafkaPartition()),
				new OffsetAndMetadata(sinkRecord.kafkaOffset()));

		logger.info("Cutting JCSMP upstream");
		Latency lag = jcsmpProxyContext.getProxy().toxics()
				.latency("lag", ToxicDirection.UPSTREAM, TimeUnit.HOURS.toMillis(1));

		Future<?> future = executorService.submit(() -> {
			String logLine;
			do {
				try {
					logLine = logReader.readLine();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			} while (!logLine.contains("Received Session Event " + SessionEvent.RECONNECTING));

			try {
				Thread.sleep(TimeUnit.SECONDS.toMillis(5));
			} catch (InterruptedException ignored) {}

			try {
				logger.info("Restoring JCSMP upstream");
				lag.remove();
				logger.info("JCSMP upstream restored");
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		});

		assertTimeoutPreemptively(Duration.ofMinutes(5), () -> {
			solaceSinkTask.put(Collections.singleton(sinkRecord));
			solaceSinkTask.flush(currentOffsets);
		});
		future.get(30, TimeUnit.SECONDS);

		ConsumerFlowProperties consumerFlowProperties = new ConsumerFlowProperties();
		consumerFlowProperties.setEndpoint(queue);
		consumerFlowProperties.setStartState(true);
		FlowReceiver flow = jcsmpSession.createFlow(null, consumerFlowProperties);
		try {
			assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
				while (true) {
					logger.info("Receiving message");
					BytesXMLMessage receivedMessage = flow.receive();
					if (receivedMessage != null) {
						assertInstanceOf(destinationType, receivedMessage.getDestination());
						assertEquals(dynamicDestinationName, receivedMessage.getDestination().getName());
						break;
					}
				}
			});
		} finally {
			flow.close();
		}
	}

	public static class BadRecordProcessor implements SolRecordProcessorIF {
		static final RuntimeException TEST_EXCEPTION = new RuntimeException("Some processing failure");

		@Override
		public BytesXMLMessage processRecord(String skey, SinkRecord record) {
			throw TEST_EXCEPTION;
		}
	}

	public static class BadSolDynamicDestinationRecordProcessor extends SolDynamicDestinationRecordProcessor {
		@Override
		public BytesXMLMessage processRecord(String skey, SinkRecord record) {
			BytesXMLMessage msg = super.processRecord(skey, record);
			try {
				msg.getProperties().putString("dynamicDestination", "abc");
			} catch (SDTException e) {
				throw new RuntimeException(e);
			}
			return msg;
		}
	}

	public static class DynamicDestinationTypeRecordProcessor extends SolSimpleRecordProcessor {
		public static final String HEADER_DYNAMIC_DESTINATION = "dynamicDestination";
		public static final String HEADER_DYNAMIC_DESTINATION_TYPE = "dynamicDestinationType";
		private static final Logger logger = LoggerFactory.getLogger(DynamicDestinationTypeRecordProcessor.class);

		@Override
		public BytesXMLMessage processRecord(String skey, SinkRecord record) {
			try {
				String dynamicDestinationName = (String) record.headers().lastWithName(HEADER_DYNAMIC_DESTINATION)
						.value();
				Class<?> dynamicDestinationType = Class.forName((String) record.headers()
						.lastWithName(HEADER_DYNAMIC_DESTINATION_TYPE).value());

				Destination dynamicDestination = dynamicDestinationType.isAssignableFrom(Queue.class) ?
						JCSMPFactory.onlyInstance().createQueue(dynamicDestinationName) :
						JCSMPFactory.onlyInstance().createTopic(dynamicDestinationName);
				logger.info("Parsed dynamic destination {} {}", dynamicDestinationType.getSimpleName(), dynamicDestination);

				BytesXMLMessage msg = super.processRecord(skey, record);
				msg.getProperties().putDestination("dynamicDestination", dynamicDestination);
				return msg;
			} catch (SDTException | ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
		}
	}
}
