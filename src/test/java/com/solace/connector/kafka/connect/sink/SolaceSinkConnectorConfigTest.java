package com.solace.connector.kafka.connect.sink;

import com.solacesystems.jcsmp.BytesXMLMessage;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyArray;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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

	@Test
	public void shouldReturnConfiguredSolRecordProcessorIFGivenConfigurableClass() {
		// GIVEN
		Map<String, String> configProps = new HashMap<>();
		configProps.put("processor.config", "dummy");
		configProps.put(SolaceSinkConstants.SOL_RECORD_PROCESSOR, TestSolRecordProcessorIF.class.getName());

		// WHEN
		SolaceSinkConnectorConfig config = new SolaceSinkConnectorConfig(configProps);

		// THEN
		SolRecordProcessorIF processor = config.getConfiguredInstance(SolaceSinkConstants.SOL_RECORD_PROCESSOR, SolRecordProcessorIF.class);
		assertNotNull(processor);
		assertNotNull(((TestSolRecordProcessorIF)processor).configs);
		assertEquals("dummy", ((TestSolRecordProcessorIF)processor).configs.get("processor.config"));

	}

	@Test
	public void testSplitTopics() {
		String[] topics = IntStream.range(0, 10)
				.mapToObj(i -> RandomStringUtils.randomAlphanumeric(30))
				.toArray(String[]::new);
		SolaceSinkConnectorConfig config = new SolaceSinkConnectorConfig(
				Collections.singletonMap(SolaceSinkConstants.SOL_TOPICS, String.join(",", topics)));
		assertNotNull(config.getTopics());
		assertArrayEquals(topics, config.getTopics());
	}

	@Test
	public void testNullTopics() {
		SolaceSinkConnectorConfig config = new SolaceSinkConnectorConfig(
				Collections.singletonMap(SolaceSinkConstants.SOL_TOPICS, null));
		assertNotNull(config.getTopics());
		assertThat(config.getTopics(), emptyArray());
	}

	public static class TestSolRecordProcessorIF implements SolRecordProcessorIF {

		Map<String, ?> configs;

		@Override
		public void configure(Map<String, ?> configs) {
			this.configs = configs;
		}

		@Override
		public BytesXMLMessage processRecord(String skey, SinkRecord record) {
			return null;
		}
	}
}
