package com.solace.connector.kafka.connect.sink;

import static org.assertj.core.api.Assertions.assertThat;

import com.solacesystems.jcsmp.JCSMPProperties;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class SolaceSessionHandlerTest {
	@ParameterizedTest
	@CsvSource({
			SolaceSinkConstants.SOL_PASSWORD + ',' + JCSMPProperties.PASSWORD,
			SolaceSinkConstants.SOL_SSL_KEY_STORE_PASSWORD + ',' + JCSMPProperties.SSL_KEY_STORE_PASSWORD,
			SolaceSinkConstants.SOL_SSL_PRIVATE_KEY_PASSWORD + ',' + JCSMPProperties.SSL_PRIVATE_KEY_PASSWORD,
			SolaceSinkConstants.SOL_SSL_TRUST_STORE_PASSWORD + ',' + JCSMPProperties.SSL_TRUST_STORE_PASSWORD
	})
	void testConfigurePasswords(String connectorProperty, String jcsmpProperty) {
		Map<String, String> properties = new HashMap<>();
		properties.put(connectorProperty, RandomStringUtils.insecure().nextAlphanumeric(30));
		SolSessionHandler sessionHandler = new SolSessionHandler(new SolaceSinkConnectorConfig(properties));
		sessionHandler.configureSession();
		assertThat(sessionHandler.properties.getStringProperty(jcsmpProperty))
				.isEqualTo(properties.get(connectorProperty));
	}

	@ParameterizedTest
	@CsvSource({
			SolaceSinkConstants.SOL_PASSWORD + ',' + JCSMPProperties.PASSWORD,
			SolaceSinkConstants.SOL_SSL_KEY_STORE_PASSWORD + ',' + JCSMPProperties.SSL_KEY_STORE_PASSWORD,
			SolaceSinkConstants.SOL_SSL_PRIVATE_KEY_PASSWORD + ',' + JCSMPProperties.SSL_PRIVATE_KEY_PASSWORD,
			SolaceSinkConstants.SOL_SSL_TRUST_STORE_PASSWORD + ',' + JCSMPProperties.SSL_TRUST_STORE_PASSWORD
	})
	void testConfigureNullPasswords(String connectorProperty, String jcsmpProperty) {
		Map<String, String> properties = new HashMap<>();
		properties.put(connectorProperty, null);
		SolSessionHandler sessionHandler = new SolSessionHandler(new SolaceSinkConnectorConfig(properties));
		sessionHandler.configureSession();
		assertThat(sessionHandler.properties.getStringProperty(jcsmpProperty))
				.isEqualTo(properties.get(connectorProperty));
	}
}
