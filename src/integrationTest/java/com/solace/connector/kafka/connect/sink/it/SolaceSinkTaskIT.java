package com.solace.connector.kafka.connect.sink.it;

import com.solace.connector.kafka.connect.sink.SolRecordProcessorIF;
import com.solace.connector.kafka.connect.sink.SolaceSinkConstants;
import com.solace.connector.kafka.connect.sink.SolaceSinkSender;
import com.solace.connector.kafka.connect.sink.SolaceSinkTask;
import com.solace.connector.kafka.connect.sink.recordprocessor.SolDynamicDestinationRecordProcessor;
import com.solace.test.integration.junit.jupiter.extension.ExecutorServiceExtension;
import com.solace.test.integration.junit.jupiter.extension.ExecutorServiceExtension.ExecSvc;
import com.solace.test.integration.junit.jupiter.extension.LogCaptorExtension;
import com.solace.test.integration.junit.jupiter.extension.LogCaptorExtension.LogCaptor;
import com.solace.test.integration.junit.jupiter.extension.PubSubPlusExtension;
import com.solace.test.integration.semp.v2.SempV2Api;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnClientProfile;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnClientUsername;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnQueue;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ClosedFacilityException;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.SDTException;
import com.solacesystems.jcsmp.transaction.RollbackException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

@ExtendWith(ExecutorServiceExtension.class)
@ExtendWith(LogCaptorExtension.class)
@ExtendWith(PubSubPlusExtension.class)
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
		assertThat(thrown.getMessage(), containsString("Failed to create Transacted Session"));
		assertThat(thrown.getCause(), instanceOf(JCSMPException.class));
		assertThat(thrown.getCause().getMessage(), containsString("Router does not support transacted sessions"));
	}

	@Test
	public void testSendToTopicThrowsJCSMPException() {
		connectorProperties.put(SolaceSinkConstants.SOL_TOPICS, RandomStringUtils.randomAlphanumeric(100));
		solaceSinkTask.start(connectorProperties);

		SinkRecord sinkRecord = new SinkRecord(RandomStringUtils.randomAlphanumeric(100), 0,
				Schema.STRING_SCHEMA, RandomStringUtils.randomAlphanumeric(100),
				Schema.BYTES_SCHEMA, RandomUtils.nextBytes(10), 0);

		solaceSinkTask.stop();
		ConnectException thrown = assertThrows(ConnectException.class, () -> solaceSinkTask.put(
				Collections.singleton(sinkRecord)));
		assertThat(thrown, instanceOf(RetriableException.class));
		assertThat(thrown.getMessage(), containsString("Received exception while sending message to topic"));
		assertThat(thrown.getCause(), instanceOf(ClosedFacilityException.class));
	}

	@Test
	public void testSendToQueueThrowsJCSMPException(Queue queue) {
		connectorProperties.put(SolaceSinkConstants.SOl_QUEUE, queue.getName());
		solaceSinkTask.start(connectorProperties);

		SinkRecord sinkRecord = new SinkRecord(RandomStringUtils.randomAlphanumeric(100), 0,
				Schema.STRING_SCHEMA, RandomStringUtils.randomAlphanumeric(100),
				Schema.BYTES_SCHEMA, RandomUtils.nextBytes(10), 0);

		solaceSinkTask.stop();
		ConnectException thrown = assertThrows(ConnectException.class, () -> solaceSinkTask.put(
				Collections.singleton(sinkRecord)));
		assertThat(thrown, instanceOf(RetriableException.class));
		assertThat(thrown.getMessage(), containsString("Received exception while sending message to queue"));
		assertThat(thrown.getCause(), instanceOf(ClosedFacilityException.class));
	}

	@Test
	public void testSendToDynamicTopicThrowsJCSMPException() {
		connectorProperties.put(SolaceSinkConstants.SOL_DYNAMIC_DESTINATION, "true");
		connectorProperties.put(SolaceSinkConstants.SOL_RECORD_PROCESSOR, SolDynamicDestinationRecordProcessor.class.getName());
		solaceSinkTask.start(connectorProperties);

		SinkRecord sinkRecord = new SinkRecord(RandomStringUtils.randomAlphanumeric(100), 0,
				Schema.STRING_SCHEMA, RandomStringUtils.randomAlphanumeric(100),
				Schema.BYTES_SCHEMA, String.format("%s %s", RandomStringUtils.randomAlphanumeric(4),
						RandomStringUtils.randomAlphanumeric(100)).getBytes(StandardCharsets.UTF_8), 0);

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

	@Test
	public void testCommitRollback(SempV2Api sempV2Api, Queue queue) throws Exception {
		connectorProperties.put(SolaceSinkConstants.SOl_QUEUE, queue.getName());
		connectorProperties.put(SolaceSinkConstants.SOl_USE_TRANSACTIONS_FOR_QUEUE, Boolean.toString(true));

		String vpnName = connectorProperties.get(SolaceSinkConstants.SOL_VPN_NAME);
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
		Map<TopicPartition, OffsetAndMetadata> currentOffsets = Collections.singletonMap(
				new TopicPartition(sinkRecord.topic(), sinkRecord.kafkaPartition()),
				new OffsetAndMetadata(sinkRecord.kafkaOffset()));

		solaceSinkTask.put(Collections.singleton(sinkRecord));
		ConnectException thrown = assertThrows(ConnectException.class, () -> solaceSinkTask.flush(currentOffsets));
		assertThat(thrown.getMessage(), containsString("Error in committing transaction"));
		assertThat(thrown.getCause(), instanceOf(RollbackException.class));
		assertThat(thrown.getCause().getMessage(), containsString("Document Is Too Large"));
	}

	@Test
	public void testAutoFlushCommitRollback(SempV2Api sempV2Api, Queue queue) throws Exception {
		connectorProperties.put(SolaceSinkConstants.SOl_QUEUE, queue.getName());
		connectorProperties.put(SolaceSinkConstants.SOl_USE_TRANSACTIONS_FOR_QUEUE, Boolean.toString(true));
		connectorProperties.put(SolaceSinkConstants.SOL_QUEUE_MESSAGES_AUTOFLUSH_SIZE, Integer.toString(1));

		String vpnName = connectorProperties.get(SolaceSinkConstants.SOL_VPN_NAME);
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

		ConnectException thrown = assertThrows(RetriableException.class, () -> solaceSinkTask.put(Collections.singleton(sinkRecord)));
		assertThat(thrown.getMessage(), containsString("Error in committing transaction"));
		assertThat(thrown.getCause(), instanceOf(RollbackException.class));
		assertThat(thrown.getCause().getMessage(), containsString("Document Is Too Large"));
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
}
