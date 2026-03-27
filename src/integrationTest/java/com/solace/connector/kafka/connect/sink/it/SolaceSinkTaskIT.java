package com.solace.connector.kafka.connect.sink.it;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import com.solace.connector.kafka.connect.sink.SolRecordProcessorIF;
import com.solace.connector.kafka.connect.sink.SolSessionEventCallbackHandler;
import com.solace.connector.kafka.connect.sink.SolaceSinkConstants;
import com.solace.connector.kafka.connect.sink.SolaceSinkSender;
import com.solace.connector.kafka.connect.sink.SolaceSinkTask;
import com.solace.connector.kafka.connect.sink.recordprocessor.SolDynamicDestinationRecordProcessor;
import com.solace.connector.kafka.connect.sink.recordprocessor.SolSimpleRecordProcessor;
import com.solace.test.integration.junit.jupiter.extension.ExecutorServiceExtension;
import com.solace.test.integration.junit.jupiter.extension.ExecutorServiceExtension.ExecSvc;
import com.solace.test.integration.junit.jupiter.extension.LogCaptorExtension;
import com.solace.test.integration.junit.jupiter.extension.LogCaptorExtension.LogCaptor;
import com.solace.test.integration.junit.jupiter.extension.PubSubPlusExtension;
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
import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith(ExecutorServiceExtension.class)
@ExtendWith(LogCaptorExtension.class)
@ExtendWith(PubSubPlusExtension.class)
class SolaceSinkTaskIT {
	private SolaceSinkTask solaceSinkTask;
	private Map<String, String> connectorProperties;
	private String clientProfileName;
	private String clientUsernameName;

	private static final Logger logger = LoggerFactory.getLogger(SolaceSinkTaskIT.class);

	@BeforeEach
	void setUp(JCSMPProperties jcsmpProperties, SempV2Api sempV2Api) throws Exception {
		solaceSinkTask = new SolaceSinkTask();
		String msgVpnName = jcsmpProperties.getStringProperty(JCSMPProperties.VPN_NAME);

		clientProfileName = sempV2Api.config().createMsgVpnClientProfile(msgVpnName, new ConfigMsgVpnClientProfile()
						.allowGuaranteedMsgSendEnabled(true)
						.allowGuaranteedMsgReceiveEnabled(true)
						.allowTransactedSessionsEnabled(true)
						.clientProfileName(RandomStringUtils.insecure().nextAlphanumeric(30)), null, null)
				.getData()
				.getClientProfileName();
		logger.info("Created client profile {}", clientProfileName);

		ConfigMsgVpnClientUsername clientUsername = sempV2Api.config().createMsgVpnClientUsername(msgVpnName,
						new ConfigMsgVpnClientUsername()
								.clientUsername(RandomStringUtils.insecure().nextAlphanumeric(30))
								.clientProfileName(clientProfileName)
								.enabled(true), null, null)
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
	void testNoProvidedMessageProcessor() {
		connectorProperties.remove(SolaceSinkConstants.SOL_RECORD_PROCESSOR);
		assertThatThrownBy(() -> solaceSinkTask.start(connectorProperties))
				.isInstanceOf(ConnectException.class)
				.hasMessageContaining("Failed to setup sender to PubSub+")
				.cause()
				.isInstanceOf(KafkaException.class)
				.hasMessageContaining("Could not find a public no-argument constructor for %s",
						SolRecordProcessorIF.class.getName());
	}

	@ParameterizedTest(name = "[{index}] transacted={0}")
	@ValueSource(booleans = { true, false })
	void testFailCreateQueueProducer(boolean transacted, SempV2Api sempV2Api, Queue queue) throws Exception {
		connectorProperties.put(SolaceSinkConstants.SOl_QUEUE, queue.getName());
		connectorProperties.put(SolaceSinkConstants.SOl_USE_TRANSACTIONS_FOR_QUEUE, Boolean.toString(transacted));

		sempV2Api.config().updateMsgVpnClientProfile(connectorProperties.get(SolaceSinkConstants.SOL_VPN_NAME),
				clientProfileName,
				new ConfigMsgVpnClientProfile().allowGuaranteedMsgSendEnabled(false), null, null);

		assertThatThrownBy(() -> solaceSinkTask.start(connectorProperties))
				.isInstanceOf(ConnectException.class)
				.hasMessageContaining("Failed to setup sender to PubSub+")
				.cause()
				.isInstanceOf(JCSMPException.class)
				.hasMessageContaining("Router does not support guaranteed publisher flows");
	}

	@Test
	void testFailTransactedSessionCreation(SempV2Api sempV2Api, Queue queue) throws Exception {
		connectorProperties.put(SolaceSinkConstants.SOl_QUEUE, queue.getName());
		connectorProperties.put(SolaceSinkConstants.SOl_USE_TRANSACTIONS_FOR_QUEUE, "true");

		sempV2Api.config().updateMsgVpnClientProfile(connectorProperties.get(SolaceSinkConstants.SOL_VPN_NAME),
				clientProfileName,
				new ConfigMsgVpnClientProfile().allowTransactedSessionsEnabled(false), null, null);

		assertThatThrownBy(() -> solaceSinkTask.start(connectorProperties))
				.isInstanceOf(ConnectException.class)
				.cause()
				.isInstanceOf(JCSMPException.class)
				.hasMessageContaining("Router does not support transacted sessions");
	}

	@ParameterizedTest
	@ValueSource(classes = {Queue.class, Topic.class})
	void testSendThrowsJCSMPException(Class<Destination> destinationType, Queue queue) {
		if (destinationType.isAssignableFrom(Queue.class)) {
			connectorProperties.put(SolaceSinkConstants.SOl_QUEUE, queue.getName());
		} else {
			connectorProperties.put(SolaceSinkConstants.SOL_TOPICS, RandomStringUtils.insecure().nextAlphanumeric(100));
		}

		solaceSinkTask.start(connectorProperties);

		SinkRecord sinkRecord = new SinkRecord(RandomStringUtils.insecure().nextAlphanumeric(100), 0,
				Schema.STRING_SCHEMA, RandomStringUtils.insecure().nextAlphanumeric(100),
				Schema.BYTES_SCHEMA, RandomUtils.insecure().randomBytes(10), 0);

		solaceSinkTask.stop();
		assertThatThrownBy(() -> solaceSinkTask.put(Collections.singleton(sinkRecord)))
				.isInstanceOf(ConnectException.class)
				.hasMessageContaining("Received exception while sending message to %s",
						destinationType.isAssignableFrom(Queue.class) ? "queue" : "topic")
				.hasCauseInstanceOf(ClosedFacilityException.class);
	}

	@ParameterizedTest(name = "[{index}] destinationType={0}")
	@ValueSource(classes = {Queue.class, Topic.class})
	void testDynamicSendThrowsJCSMPException(Class<Destination> destinationType, Queue queue) {
		connectorProperties.put(SolaceSinkConstants.SOL_DYNAMIC_DESTINATION, Boolean.toString(true));
		connectorProperties.put(SolaceSinkConstants.SOL_RECORD_PROCESSOR, DynamicDestinationTypeRecordProcessor.class
				.getName());
		solaceSinkTask.start(connectorProperties);

		SinkRecord sinkRecord = new SinkRecord(RandomStringUtils.insecure().nextAlphanumeric(100), 0,
				Schema.STRING_SCHEMA, RandomStringUtils.insecure().nextAlphanumeric(100),
				Schema.BYTES_SCHEMA, RandomUtils.insecure().randomBytes(10), 0);

		String dynamicDestinationName = destinationType.isAssignableFrom(Queue.class) ? queue.getName() :
				RandomStringUtils.insecure().nextAlphanumeric(100);
		sinkRecord.headers()
				.addString(DynamicDestinationTypeRecordProcessor.HEADER_DYNAMIC_DESTINATION, dynamicDestinationName)
				.addString(DynamicDestinationTypeRecordProcessor.HEADER_DYNAMIC_DESTINATION_TYPE, destinationType.getName());

		solaceSinkTask.stop();
		assertThatThrownBy(() -> solaceSinkTask.put(Collections.singleton(sinkRecord)))
				.isInstanceOf(ConnectException.class)
				.hasMessageContaining("Received exception while sending message to topic")
				.hasCauseInstanceOf(ClosedFacilityException.class);
	}

	@ParameterizedTest(name = "[{index}] ignoreRecordProcessorError={0}")
	@ValueSource(booleans = { true, false })
	void testInvalidDynamicDestination(boolean ignoreRecordProcessorError,
											  @ExecSvc ExecutorService executorService,
											  @LogCaptor(SolaceSinkSender.class) BufferedReader logReader) {
		connectorProperties.put(SolaceSinkConstants.SOL_RECORD_PROCESSOR, BadSolDynamicDestinationRecordProcessor.class.getName());
		connectorProperties.put(SolaceSinkConstants.SOL_RECORD_PROCESSOR_IGNORE_ERROR, Boolean.toString(ignoreRecordProcessorError));
		connectorProperties.put(SolaceSinkConstants.SOL_DYNAMIC_DESTINATION, Boolean.toString(true));
		solaceSinkTask.start(connectorProperties);

		Set<SinkRecord> records = Collections.singleton(new SinkRecord(RandomStringUtils.insecure().nextAlphanumeric(100), 0,
				Schema.STRING_SCHEMA, RandomStringUtils.insecure().nextAlphanumeric(100),
				Schema.BYTES_SCHEMA, String.format("%s %s", RandomStringUtils.insecure().nextAlphanumeric(4),
				RandomStringUtils.insecure().nextAlphanumeric(100)).getBytes(StandardCharsets.UTF_8), 0));

		if (ignoreRecordProcessorError) {
			Future<?> future = executorService.submit((Callable<?>) () -> {
				String logLine;
				do {
					logLine = logReader.readLine();
				} while (!logLine.contains("Received exception retrieving Dynamic Destination"));
				return null;
			});
			solaceSinkTask.put(records);
			assertThat(future).succeedsWithin(30, TimeUnit.SECONDS);
		} else {
			assertThatThrownBy(() -> solaceSinkTask.put(records))
					.isInstanceOf(ConnectException.class)
					.hasMessageContaining("Received exception retrieving Dynamic Destination")
					.cause()
					.isInstanceOf(SDTException.class)
					.hasMessageContaining("No conversion from String to Destination");
		}
	}

	@ParameterizedTest(name = "[{index}] ignoreRecordProcessorError={0}")
	@ValueSource(booleans = { true, false })
	void testRecordProcessorError(boolean ignoreRecordProcessorError,
										 @ExecSvc ExecutorService executorService,
										 @LogCaptor(SolaceSinkSender.class) BufferedReader logReader) {
		connectorProperties.put(SolaceSinkConstants.SOL_RECORD_PROCESSOR, BadRecordProcessor.class.getName());
		connectorProperties.put(SolaceSinkConstants.SOL_RECORD_PROCESSOR_IGNORE_ERROR, Boolean.toString(ignoreRecordProcessorError));
		solaceSinkTask.start(connectorProperties);

		Set<SinkRecord> records = Collections.singleton(new SinkRecord(RandomStringUtils.insecure().nextAlphanumeric(100), 0,
				Schema.STRING_SCHEMA, RandomStringUtils.insecure().nextAlphanumeric(100),
				Schema.BYTES_SCHEMA, RandomUtils.insecure().randomBytes(10), 0));

		if (ignoreRecordProcessorError) {
			Future<?> future = executorService.submit((Callable<?>) () -> {
				String logLine;
				do {
					logLine = logReader.readLine();
				} while (!logLine.contains("Encountered exception in record processing"));
				return null;
			});
			solaceSinkTask.put(records);
			assertThat(future).succeedsWithin(30, TimeUnit.SECONDS);
		} else {
			assertThatThrownBy(() -> solaceSinkTask.put(records))
					.isInstanceOf(ConnectException.class)
					.hasMessageContaining("Encountered exception in record processing")
					.hasCause(BadRecordProcessor.TEST_EXCEPTION);
		}
	}

	@ParameterizedTest(name = "[{index}] autoFlush={0}")
	@ValueSource(booleans = {false, true})
	void testCommitRollback(boolean autoFlush, SempV2Api sempV2Api, Queue queue) throws Exception {
		connectorProperties.put(SolaceSinkConstants.SOl_QUEUE, queue.getName());
		connectorProperties.put(SolaceSinkConstants.SOL_TOPICS, RandomStringUtils.insecure().nextAlphanumeric(100));
		connectorProperties.put(SolaceSinkConstants.SOl_USE_TRANSACTIONS_FOR_QUEUE, Boolean.toString(true));
		connectorProperties.put(SolaceSinkConstants.SOl_USE_TRANSACTIONS_FOR_TOPICS, Boolean.toString(true));
		if (autoFlush) {
			connectorProperties.put(SolaceSinkConstants.SOL_AUTOFLUSH_SIZE, Integer.toString(2));
		}

		String vpnName = connectorProperties.get(SolaceSinkConstants.SOL_VPN_NAME);
		sempV2Api.config().createMsgVpnQueueSubscription(vpnName, queue.getName(), new ConfigMsgVpnQueueSubscription()
				.subscriptionTopic(connectorProperties.get(SolaceSinkConstants.SOL_TOPICS)), null, null);
		sempV2Api.config().updateMsgVpnQueue(vpnName, queue.getName(), new ConfigMsgVpnQueue().maxMsgSize(1), null, null);

		await("queue max message size to be updated to 1")
			.atMost(20, SECONDS)
			.pollInterval(100, TimeUnit.MILLISECONDS)
			.until(() -> {
				logger.info("Waiting for queue {} to have max message size of 1", queue.getName());
				return sempV2Api.monitor().getMsgVpnQueue(vpnName, queue.getName(), null).getData()
						.getMaxMsgSize() == 1;
			});

		solaceSinkTask.start(connectorProperties);

		SinkRecord sinkRecord = new SinkRecord(RandomStringUtils.insecure().nextAlphanumeric(100), 0,
				Schema.STRING_SCHEMA, RandomStringUtils.insecure().nextAlphanumeric(100),
				Schema.BYTES_SCHEMA, RandomUtils.insecure().randomBytes(10), 0);

		ConnectException thrown;
		if (autoFlush) {
			thrown = assertThrows(ConnectException.class, () -> solaceSinkTask.put(Collections.singleton(sinkRecord)));
		} else {
			Map<TopicPartition, OffsetAndMetadata> currentOffsets = Collections.singletonMap(
					new TopicPartition(sinkRecord.topic(), sinkRecord.kafkaPartition()),
					new OffsetAndMetadata(sinkRecord.kafkaOffset()));
			solaceSinkTask.put(Collections.singleton(sinkRecord));
			thrown = assertThrows(ConnectException.class, () -> solaceSinkTask.flush(currentOffsets));
		}

		assertThat(thrown)
				.hasMessageContaining("Error in committing transaction")
				.cause()
				.isInstanceOf(RollbackException.class)
				.hasMessageContaining("Document Is Too Large");

		// If the txn fails and needs to rollback, the API might not try to send subsequent messages to the broker.
		// Resulting in only 1 failed message being reported by the broker.
		assertThat(sempV2Api.monitor().getMsgVpnQueue(vpnName, queue.getName(), null).getData()
						.getMaxMsgSizeExceededDiscardedMsgCount())
				.isIn(1L, 2L);
	}

	@CartesianTest(name = "[{index}] destinationType={0}, autoFlush={1}")
	void testDynamicDestinationCommitRollback(
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

		String topicName = RandomStringUtils.insecure().nextAlphanumeric(100);
		String vpnName = connectorProperties.get(SolaceSinkConstants.SOL_VPN_NAME);

		if (destinationType.isAssignableFrom(Topic.class)) {
			sempV2Api.config().createMsgVpnQueueSubscription(vpnName, queue.getName(),
					new ConfigMsgVpnQueueSubscription().subscriptionTopic(topicName), null, null);
		}

		sempV2Api.config().updateMsgVpnQueue(vpnName, queue.getName(), new ConfigMsgVpnQueue().maxMsgSize(1), null, null);
		await("queue max message size to be updated to 1")
			.atMost(20, SECONDS)
			.pollInterval(100, TimeUnit.MILLISECONDS)
			.until(() -> {
				logger.info("Waiting for queue {} to have max message size of 1", queue.getName());
				return sempV2Api.monitor().getMsgVpnQueue(vpnName, queue.getName(), null).getData()
						.getMaxMsgSize() == 1;
			});

		solaceSinkTask.start(connectorProperties);

		SinkRecord sinkRecord = new SinkRecord(RandomStringUtils.insecure().nextAlphanumeric(100), 0,
				Schema.STRING_SCHEMA, RandomStringUtils.insecure().nextAlphanumeric(100),
				Schema.BYTES_SCHEMA, RandomUtils.insecure().randomBytes(10), 0);

		String dynamicDestinationName = destinationType.isAssignableFrom(Queue.class) ? queue.getName() : topicName;
		sinkRecord.headers()
				.addString(DynamicDestinationTypeRecordProcessor.HEADER_DYNAMIC_DESTINATION, dynamicDestinationName)
				.addString(DynamicDestinationTypeRecordProcessor.HEADER_DYNAMIC_DESTINATION_TYPE, destinationType.getName());

		ConnectException thrown;
		if (autoFlush) {
			thrown = assertThrows(ConnectException.class, () -> solaceSinkTask.put(Collections.singleton(sinkRecord)));
		} else {
			Map<TopicPartition, OffsetAndMetadata> currentOffsets = Collections.singletonMap(
					new TopicPartition(sinkRecord.topic(), sinkRecord.kafkaPartition()),
					new OffsetAndMetadata(sinkRecord.kafkaOffset()));
			solaceSinkTask.put(Collections.singleton(sinkRecord));
			thrown = assertThrows(ConnectException.class, () -> solaceSinkTask.flush(currentOffsets));
		}

		assertThat(thrown)
				.hasMessageContaining("Error in committing transaction")
				.cause()
				.isInstanceOf(RollbackException.class)
				.hasMessageContaining("Document Is Too Large");
		assertThat(sempV2Api.monitor().getMsgVpnQueue(vpnName, queue.getName(), null).getData()
				.getMaxMsgSizeExceededDiscardedMsgCount())
				.isEqualTo(1);
	}

	@Disabled()
	@ParameterizedTest(name = "[{index}] autoFlush={0}")
	@ValueSource(booleans = {false, true})
	void testLongCommit(boolean autoFlush,
							   @JCSMPProxy JCSMPSession jcsmpSession,
							   SempV2Api sempV2Api,
							   Queue queue,
							   @JCSMPProxy ToxiproxyContext jcsmpProxyContext,
							   @ExecSvc ExecutorService executorService,
							   @LogCaptor(SolSessionEventCallbackHandler.class) BufferedReader logReader)
			throws Exception {
		connectorProperties.put(SolaceSinkConstants.SOL_HOST, (String) jcsmpSession.getProperty(JCSMPProperties.HOST));
		connectorProperties.put(SolaceSinkConstants.SOl_QUEUE, queue.getName());
		connectorProperties.put(SolaceSinkConstants.SOL_TOPICS, RandomStringUtils.insecure().nextAlphanumeric(100));
		connectorProperties.put(SolaceSinkConstants.SOl_USE_TRANSACTIONS_FOR_QUEUE, Boolean.toString(true));
		connectorProperties.put(SolaceSinkConstants.SOl_USE_TRANSACTIONS_FOR_TOPICS, Boolean.toString(true));
		connectorProperties.put(SolaceSinkConstants.SOL_CHANNEL_PROPERTY_reconnectRetries, Integer.toString(-1));

		if (autoFlush) {
			connectorProperties.put(SolaceSinkConstants.SOL_AUTOFLUSH_SIZE, Integer.toString(2));
		}

		String vpnName = connectorProperties.get(SolaceSinkConstants.SOL_VPN_NAME);
		sempV2Api.config().createMsgVpnQueueSubscription(vpnName, queue.getName(), new ConfigMsgVpnQueueSubscription()
				.subscriptionTopic(connectorProperties.get(SolaceSinkConstants.SOL_TOPICS)), null, null);

		solaceSinkTask.start(connectorProperties);

		SinkRecord sinkRecord = new SinkRecord(RandomStringUtils.insecure().nextAlphanumeric(100), 0,
				Schema.STRING_SCHEMA, RandomStringUtils.insecure().nextAlphanumeric(100),
				Schema.BYTES_SCHEMA, RandomUtils.insecure().randomBytes(10), 0);
		Map<TopicPartition, OffsetAndMetadata> currentOffsets = Collections.singletonMap(
				new TopicPartition(sinkRecord.topic(), sinkRecord.kafkaPartition()),
				new OffsetAndMetadata(sinkRecord.kafkaOffset()));

		logger.info("Cutting JCSMP upstream");
		Latency lag = jcsmpProxyContext.getProxy().toxics()
				.latency("lag", ToxicDirection.UPSTREAM, TimeUnit.HOURS.toMillis(1));

		Future<?> future = executorService.submit((Callable<?>) () -> {
			String logLine;
			do {
				logLine = logReader.readLine();
			} while (!logLine.contains("Received Session Event " + SessionEvent.RECONNECTING));

			Thread.sleep(TimeUnit.SECONDS.toMillis(5));

			logger.info("Restoring JCSMP upstream");
			lag.remove();
			logger.info("JCSMP upstream restored");

			return null;
		});

		assertTimeoutPreemptively(Duration.ofMinutes(5), () -> {
			solaceSinkTask.put(Collections.singleton(sinkRecord));
			solaceSinkTask.flush(currentOffsets);
		});
		assertThat(future).succeedsWithin(30, TimeUnit.SECONDS);

		List<Destination> receivedDestinations = new ArrayList<>();
		ConsumerFlowProperties consumerFlowProperties = new ConsumerFlowProperties();
		consumerFlowProperties.setEndpoint(queue);
		consumerFlowProperties.setStartState(true);
		FlowReceiver flow = jcsmpSession.createFlow(null, consumerFlowProperties);
		try {
			await("messages to be received")
				.atMost(30, SECONDS)
				.until(() -> {
					logger.info("Receiving messages");
					Optional.ofNullable(flow.receive())
							.map(XMLMessage::getDestination)
							.ifPresent(receivedDestinations::add);
					return receivedDestinations.size() >= 2;
				});
		} finally {
			flow.close();
		}

		assertThat(receivedDestinations).containsExactlyInAnyOrder(
				queue,
				JCSMPFactory.onlyInstance().createTopic(connectorProperties.get(SolaceSinkConstants.SOL_TOPICS)));
	}

	@Disabled()
	@CartesianTest(name = "[{index}] destinationType={0}, autoFlush={1}")
	void testDynamicDestinationLongCommit(
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
		connectorProperties.put(SolaceSinkConstants.SOL_TOPICS, RandomStringUtils.insecure().nextAlphanumeric(100));

		if (autoFlush) {
			connectorProperties.put(SolaceSinkConstants.SOL_AUTOFLUSH_SIZE, Integer.toString(1));
		}

		String topicName = RandomStringUtils.insecure().nextAlphanumeric(100);
		if (destinationType.isAssignableFrom(Topic.class)) {
			sempV2Api.config().createMsgVpnQueueSubscription(connectorProperties.get(SolaceSinkConstants.SOL_VPN_NAME),
					queue.getName(), new ConfigMsgVpnQueueSubscription()
							.subscriptionTopic(topicName), null, null);
		}

		solaceSinkTask.start(connectorProperties);

		SinkRecord sinkRecord = new SinkRecord(RandomStringUtils.insecure().nextAlphanumeric(100), 0,
				Schema.STRING_SCHEMA, RandomStringUtils.insecure().nextAlphanumeric(100),
				Schema.BYTES_SCHEMA, RandomUtils.insecure().randomBytes(10), 0);

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

		Future<?> future = executorService.submit((Callable<?>) () -> {
			String logLine;
			do {
					logLine = logReader.readLine();
			} while (!logLine.contains("Received Session Event " + SessionEvent.RECONNECTING));

			Thread.sleep(TimeUnit.SECONDS.toMillis(5));

			logger.info("Restoring JCSMP upstream");
			lag.remove();
			logger.info("JCSMP upstream restored");

			return null;
		});

		assertTimeoutPreemptively(Duration.ofMinutes(5), () -> {
			solaceSinkTask.put(Collections.singleton(sinkRecord));
			solaceSinkTask.flush(currentOffsets);
		});
		assertThat(future).succeedsWithin(30, TimeUnit.SECONDS);

		ConsumerFlowProperties consumerFlowProperties = new ConsumerFlowProperties();
		consumerFlowProperties.setEndpoint(queue);
		consumerFlowProperties.setStartState(true);
		FlowReceiver flow = jcsmpSession.createFlow(null, consumerFlowProperties);
		try {
				logger.info("Receiving message");
				BytesXMLMessage receivedMessage = flow.receive(30000);
				assertInstanceOf(destinationType, receivedMessage.getDestination());
				assertEquals(dynamicDestinationName, receivedMessage.getDestination().getName());
		} finally {
			flow.close();
		}
	}

	public static class BadRecordProcessor implements SolRecordProcessorIF {
		static final RuntimeException TEST_EXCEPTION = new RuntimeException("Some processing failure");

		@Override
		public BytesXMLMessage processRecord(String skey, SinkRecord sinkRecord) {
			throw TEST_EXCEPTION;
		}
	}

	public static class BadSolDynamicDestinationRecordProcessor extends SolDynamicDestinationRecordProcessor {
		@Override
		public BytesXMLMessage processRecord(String skey, SinkRecord sinkRecord) {
			BytesXMLMessage msg = super.processRecord(skey, sinkRecord);
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
		public BytesXMLMessage processRecord(String skey, SinkRecord sinkRecord) {
			try {
				String dynamicDestinationName = (String) sinkRecord.headers().lastWithName(HEADER_DYNAMIC_DESTINATION)
						.value();
				Class<?> dynamicDestinationType = Class.forName((String) sinkRecord.headers()
						.lastWithName(HEADER_DYNAMIC_DESTINATION_TYPE).value());

				Destination dynamicDestination = dynamicDestinationType.isAssignableFrom(Queue.class) ?
						JCSMPFactory.onlyInstance().createQueue(dynamicDestinationName) :
						JCSMPFactory.onlyInstance().createTopic(dynamicDestinationName);
				logger.info("Parsed dynamic destination {} {}", dynamicDestinationType.getSimpleName(), dynamicDestination);

				BytesXMLMessage msg = super.processRecord(skey, sinkRecord);
				msg.getProperties().putDestination("dynamicDestination", dynamicDestination);
				return msg;
			} catch (SDTException | ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
		}
	}
}
