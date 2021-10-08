package com.solace.connector.kafka.connect.sink.it;

import com.solace.connector.kafka.connect.sink.recordprocessor.SolSimpleRecordProcessor;

public interface TestConstants {
    String UNZIPPEDCONNECTORDESTINATION = "src/integrationTest/resources";
    String CONNECTORJSONPROPERTIESFILE = "etc/solace_sink_properties.json";
    String SOL_VPN = "default";
    String KAFKA_SINK_TOPIC = "kafka-test-topic-sink";
    String SOL_ROOT_TOPIC = "pubsubplus-test-topic-sink";
    String SOL_TOPICS = "pubsubplus-test-topic-sink";
    String SOL_QUEUE = "pubsubplus-test-queue-sink";
    String CONN_MSGPROC_CLASS = SolSimpleRecordProcessor.class.getName();
    String CONN_KAFKA_MSGKEY = "DESTINATION";
}
