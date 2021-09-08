package com.solace.connector.kafka.connect.sink;

import com.solacesystems.jcsmp.JCSMPException;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SolaceSinkTaskTest {
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
	public void testFailSessionConnect() {
		Map<String, String> props = new HashMap<>();
		ConnectException thrown = assertThrows(ConnectException.class, () -> solaceSinkTask.start(props));
		assertThat(thrown.getMessage(), containsString("Failed to create JCSMPSession"));
		assertThat(thrown.getCause(), instanceOf(JCSMPException.class));
		assertThat(thrown.getCause().getMessage(), containsString("Null value was passed in for property (host)"));
	}
}
