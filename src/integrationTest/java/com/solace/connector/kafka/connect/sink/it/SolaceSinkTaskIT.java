package com.solace.connector.kafka.connect.sink.it;

import com.solace.connector.kafka.connect.sink.SolRecordProcessorIF;
import com.solace.connector.kafka.connect.sink.SolaceSinkConstants;
import com.solace.connector.kafka.connect.sink.SolaceSinkTask;
import com.solace.test.integration.junit.jupiter.extension.PubSubPlusExtension;
import com.solace.test.integration.semp.v2.SempV2Api;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnClientProfile;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnClientUsername;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.Queue;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
}
