package com.solace.connector.kafka.connect.sink;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.solacesystems.jcsmp.JCSMPException;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SolaceSinkTaskTest {
	private SolaceSinkTask solaceSinkTask;

	@BeforeEach
	void setUp() {
		solaceSinkTask = new SolaceSinkTask();
	}

	@AfterEach
	void tearDown() {
		solaceSinkTask.stop();
	}

	@Test
	void testFailSessionConnect() {
		Map<String, String> props = new HashMap<>();
		assertThatThrownBy(() -> solaceSinkTask.start(props))
				.isInstanceOf(ConnectException.class)
				.hasMessageContaining("Failed to create JCSMPSession")
				.cause()
				.isInstanceOf(JCSMPException.class)
				.hasMessageContaining("Null value was passed in for property (host)");
	}
}
