package com.solace.connector.kafka.connect.sink;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.kafka.common.config.types.Password;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SolaceSinkConnectorConfigTest {
	@ParameterizedTest
	@ValueSource(strings = {
			SolaceSinkConstants.SOL_PASSWORD,
			SolaceSinkConstants.SOL_SSL_KEY_STORE_PASSWORD,
			SolaceSinkConstants.SOL_SSL_PRIVATE_KEY_PASSWORD,
			SolaceSinkConstants.SOL_SSL_TRUST_STORE_PASSWORD
	})
	public void testPasswordsObfuscation(String property) {
		Map<String, String> properties = new HashMap<>();
		properties.put(property, RandomStringUtils.randomAlphanumeric(30));
		SolaceSinkConnectorConfig config = new SolaceSinkConnectorConfig(properties);
		Password password = config.getPassword(property);
		assertEquals(Password.HIDDEN, password.toString());
		assertEquals(properties.get(property), password.value());
	}
}
