package com.solace.connector.kafka.connect.sink;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class VersionUtilTest {
	@Test
	void testGetVersion() {
		assertThat(VersionUtil.getVersion()).matches("^[0-9]+\\.[0-9]+\\.[0-9]+$");
	}
}
