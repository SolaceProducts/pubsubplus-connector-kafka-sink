package com.solace.messaging.kafka.it;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.Message;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.SDTException;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.impl.AbstractDestination;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class SinkConnectorIT implements TestConstants {

    static Logger logger = LoggerFactory.getLogger(SinkConnectorIT.class.getName());
    static TestSolaceConsumer solaceConsumer = new TestSolaceConsumer("tcp://" + MessagingServiceFullLocalSetupConfluent.COMPOSE_CONTAINER_PUBSUBPLUS
                            .getServiceHost("solbroker_1", 55555) + ":55555", "default", "default", "default");
    // Connectordeployment creates a Kafka topic "kafkaTestTopic", which is used next
    static SolaceConnectorDeployment connectorDeployment = new SolaceConnectorDeployment();
    static TestKafkaProducer kafkaProducer = new TestKafkaProducer(connectorDeployment.kafkaTestTopic);
    static enum AdditionalCheck { ATTACHMENTBYTEBUFFER, CORRELATIONID }
    
    ////////////////////////////////////////////////////
    // Main setup/teardown

    @BeforeAll
    static void setUp() {
         try {
             // Start consumer
             // Ensure test queue exists on PubSub+
            solaceConsumer.provisionQueue(SOL_QUEUE);
            solaceConsumer.start();
            // Start producer
            kafkaProducer.start();
        } catch (JCSMPException e1) {
            e1.printStackTrace();
        }
    }

    @AfterAll
    static void cleanUp() {
        kafkaProducer.close();
        solaceConsumer.stop();
    }

    
    
    ////////////////////////////////////////////////////
    // Test types
    
    void messageToKafkaTest(String expectedSolaceQueue, String[] expectedSolaceTopics, String kafkaKey, String kafkaValue,
                    Map<AdditionalCheck, String> additionalChecks) {
        try {
            // Clean catch queues first
            // TODO: fix possible concurrency issue with cleaning/wring the queue later
            TestSolaceConsumer.solaceReceivedQueueMessages.clear();
            TestSolaceConsumer.solaceReceivedTopicMessages.clear();
            
            // Received messages
            List<BytesXMLMessage> receivedMessages = new ArrayList<>();
            
            // Send Kafka message
            RecordMetadata metadata = kafkaProducer.sendMessageToKafka(kafkaKey, kafkaValue);
            assertNotNull(metadata);
            
            // Wait for PubSub+ to report messages - populate queue and topics if provided
            if (expectedSolaceQueue != null) {
                BytesXMLMessage queueMessage = TestSolaceConsumer.solaceReceivedQueueMessages.poll(5,TimeUnit.SECONDS);
                assertNotNull(queueMessage);
                receivedMessages.add(queueMessage);
            } else {
                assert(TestSolaceConsumer.solaceReceivedQueueMessages.size() == 0);
            }
            for(String s : expectedSolaceTopics) {
                BytesXMLMessage newTopicMessage = TestSolaceConsumer.solaceReceivedTopicMessages.poll(5,TimeUnit.SECONDS);
                assertNotNull(newTopicMessage);
                receivedMessages.add(newTopicMessage);
             }

            // Evaluate messages
            // ensure each solacetopic got a respective message
            for(String topicname : expectedSolaceTopics) {
                boolean topicFound = false;
                for (BytesXMLMessage message : receivedMessages) {
                    if (message.getDestination().getName().equals(topicname)) {
                        topicFound = true;
                        break;
                    }
                }
                if (!topicFound) fail("Nothing was delivered to topic " + topicname);
            }
            // check message contents
            for (BytesXMLMessage message : receivedMessages) { 
                SDTMap userHeader = message.getProperties();
                assert(userHeader.getString("k_topic").contentEquals(metadata.topic()));
                assert(userHeader.getString("k_partition").contentEquals(Long.toString(metadata.partition())));
                assert(userHeader.getString("k_offset").contentEquals(Long.toString(metadata.offset())));
                assert(message.getApplicationMessageType().contains(metadata.topic()));
                // additional checks as requested
                if (additionalChecks != null) {
                    for (Map.Entry<AdditionalCheck, String> check : additionalChecks.entrySet()) {
                        if (check.getKey() == AdditionalCheck.ATTACHMENTBYTEBUFFER) {
                            // Verify contents of the message AttachmentByteBuffer
                            assert(Arrays.equals((byte[])message.getAttachmentByteBuffer().array(),check.getValue().getBytes()));
                        }
                        if (check.getKey() == AdditionalCheck.CORRELATIONID) {
                            // Verify contents of the message correlationId
                            assert(message.getCorrelationId().contentEquals(check.getValue()));
                        }
                    }
                }
            }
            
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (SDTException e) {
            e.printStackTrace();
        }
    }
    
    ////////////////////////////////////////////////////
    // Scenarios
    
    @DisplayName("Sink SimpleMessageProcessor Perf tests")
    @Nested
    @TestInstance(Lifecycle.PER_CLASS)
    @Disabled
    class SinkConnectorSimpleMessageProcessorPerfTests {
        
        String topics[] = {SOL_ROOT_TOPIC+"/TestTopic1/SubTopic"};
        
        @BeforeAll
        void setUp() {
            Properties prop = new Properties();
            prop.setProperty("sol.record_processor_class", "com.solace.sink.connector.recordprocessor.SolSimpleRecordProcessor");
            prop.setProperty("sol.dynamic_destination", "false");
            prop.setProperty("sol.topics", String.join(", ", topics));
            connectorDeployment.startConnector(prop);
        }


        @DisplayName("TextMessage-QueueAndTopics-SolSampleSimpleMessageProcessor")
        @Test
        void kafkaConsumerTextMessageToTopicTest() {
            messageToKafkaTest(null, topics,
                            // kafka key and value
                            "Key", "Hello TextMessageToTopicTest world!",
                            // additional checks
                            ImmutableMap.of(AdditionalCheck.ATTACHMENTBYTEBUFFER, "Hello TextMessageToTopicTest world!"));
        }
    }
    
    @DisplayName("Sink SimpleMessageProcessor tests")
    @Nested
    @TestInstance(Lifecycle.PER_CLASS)
    class SinkConnectorSimpleMessageProcessorTests {
        
        String topics[] = {SOL_ROOT_TOPIC+"/TestTopic1/SubTopic", SOL_ROOT_TOPIC+"/TestTopic2/SubTopic"};
        
        @BeforeAll
        void setUp() {
            Properties prop = new Properties();
            prop.setProperty("sol.record_processor_class", "com.solace.sink.connector.recordprocessor.SolSimpleRecordProcessor");
            prop.setProperty("sol.dynamic_destination", "false");
            prop.setProperty("sol.topics", String.join(", ", topics));
            prop.setProperty("sol.queue", SOL_QUEUE);
            connectorDeployment.startConnector(prop);
        }


        @DisplayName("TextMessage-QueueAndTopics-SolSampleSimpleMessageProcessor")
        @Test
        void kafkaConsumerTextMessageToTopicTest() {
            messageToKafkaTest(SOL_QUEUE, topics,
                            // kafka key and value
                            "Key", "Hello TextMessageToTopicTest world!",
                            // additional checks
                            ImmutableMap.of(AdditionalCheck.ATTACHMENTBYTEBUFFER, "Hello TextMessageToTopicTest world!"));
        }
    }
    
    
    @DisplayName("Sink KeyedMessageProcessor-NONE tests")
    @Nested
    @TestInstance(Lifecycle.PER_CLASS)
    class SinkConnectorNoneKeyedMessageProcessorTests {
        
        String topics[] = {SOL_ROOT_TOPIC+"/TestTopic1/SubTopic", SOL_ROOT_TOPIC+"/TestTopic2/SubTopic"};
        
        @BeforeAll
        void setUp() {
            Properties prop = new Properties();
            prop.setProperty("sol.record_processor_class", "com.solace.sink.connector.recordprocessor.SolSimpleKeyedRecordProcessor");
            prop.setProperty("sol.dynamic_destination", "false");
            prop.setProperty("sol.topics", String.join(", ", topics));
            prop.setProperty("sol.kafka_message_key", "NONE");
            prop.setProperty("sol.queue", SOL_QUEUE);
            connectorDeployment.startConnector(prop);
        }


        @DisplayName("TextMessage-QueueAndTopics-KeyedMessageProcessor-NONE")
        @Test
        void kafkaConsumerTextMessageToTopicTest() {
            messageToKafkaTest(SOL_QUEUE, topics,
                            // kafka key and value
                            "Key", "Hello TextMessageToTopicTest world!",
                            // additional checks
                            ImmutableMap.of(AdditionalCheck.ATTACHMENTBYTEBUFFER, "Hello TextMessageToTopicTest world!"));
        }
    }
    

    @DisplayName("Sink KeyedMessageProcessor-DESTINATION tests")
    @Nested
    @TestInstance(Lifecycle.PER_CLASS)
    class SinkConnectorDestinationKeyedMessageProcessorTests {
        
        String topics[] = {SOL_ROOT_TOPIC+"/TestTopic1/SubTopic", SOL_ROOT_TOPIC+"/TestTopic2/SubTopic"};
        
        @BeforeAll
        void setUp() {
            Properties prop = new Properties();
            prop.setProperty("sol.record_processor_class", "com.solace.sink.connector.recordprocessor.SolSimpleKeyedRecordProcessor");
            prop.setProperty("sol.dynamic_destination", "false");
            prop.setProperty("sol.topics", String.join(", ", topics));
            prop.setProperty("sol.kafka_message_key", "DESTINATION");
            prop.setProperty("sol.queue", SOL_QUEUE);
            connectorDeployment.startConnector(prop);
        }


        @DisplayName("TextMessage-QueueAndTopics-KeyedMessageProcessor-DESTINATION")
        @Test
        void kafkaConsumerTextMessageToTopicTest() {
            messageToKafkaTest(SOL_QUEUE, topics,
                            // kafka key and value
                            "Destination", "Hello TextMessageToTopicTest world!",
                            // additional checks with expected values
                            ImmutableMap.of(AdditionalCheck.ATTACHMENTBYTEBUFFER, "Hello TextMessageToTopicTest world!",
                                            AdditionalCheck.CORRELATIONID, "Destination"));
        }
    }
    

    @DisplayName("Sink KeyedMessageProcessor-CORRELATION_ID tests")
    @Nested
    @TestInstance(Lifecycle.PER_CLASS)
    class SinkConnectorCorrelationIdKeyedMessageProcessorTests {
        
        String topics[] = {SOL_ROOT_TOPIC+"/TestTopic1/SubTopic", SOL_ROOT_TOPIC+"/TestTopic2/SubTopic"};
        
        @BeforeAll
        void setUp() {
            Properties prop = new Properties();
            prop.setProperty("sol.record_processor_class", "com.solace.sink.connector.recordprocessor.SolSimpleKeyedRecordProcessor");
            prop.setProperty("sol.dynamic_destination", "false");
            prop.setProperty("sol.topics", String.join(", ", topics));
            prop.setProperty("sol.kafka_message_key", "CORRELATION_ID");
            prop.setProperty("sol.queue", SOL_QUEUE);
            connectorDeployment.startConnector(prop);
        }


        @DisplayName("TextMessage-QueueAndTopics-KeyedMessageProcessor-CORRELATION_ID")
        @Test
        void kafkaConsumerTextMessageToTopicTest() {
            messageToKafkaTest(SOL_QUEUE, topics,
                            // kafka key and value
                            "TestCorrelationId", "Hello TextMessageToTopicTest world!",
                            // additional checks with expected values
                            ImmutableMap.of(AdditionalCheck.ATTACHMENTBYTEBUFFER, "Hello TextMessageToTopicTest world!",
                                            AdditionalCheck.CORRELATIONID, "TestCorrelationId"));
        }
    }
    

    @DisplayName("Sink KeyedMessageProcessor-CORRELATION_ID_AS_BYTES tests")
    @Nested
    @TestInstance(Lifecycle.PER_CLASS)
    class SinkConnectorCorrelationIdAsBytesKeyedMessageProcessorTests {
        
        String topics[] = {SOL_ROOT_TOPIC+"/TestTopic1/SubTopic", SOL_ROOT_TOPIC+"/TestTopic2/SubTopic"};
        
        @BeforeAll
        void setUp() {
            Properties prop = new Properties();
            prop.setProperty("sol.record_processor_class", "com.solace.sink.connector.recordprocessor.SolSimpleKeyedRecordProcessor");
            prop.setProperty("sol.dynamic_destination", "false");
            prop.setProperty("sol.topics", String.join(", ", topics));
            prop.setProperty("sol.kafka_message_key", "CORRELATION_ID_AS_BYTES");
            prop.setProperty("sol.queue", SOL_QUEUE);
            connectorDeployment.startConnector(prop);
        }


        @DisplayName("TextMessage-QueueAndTopics-KeyedMessageProcessor-CORRELATION_ID_AS_BYTES")
        @Test
        void kafkaConsumerTextMessageToTopicTest() {
            messageToKafkaTest(SOL_QUEUE, topics,
                            // kafka key and value
                            "TestCorrelationId", "Hello TextMessageToTopicTest world!",
                            // additional checks with expected values
                            ImmutableMap.of(AdditionalCheck.ATTACHMENTBYTEBUFFER, "Hello TextMessageToTopicTest world!",
                                            AdditionalCheck.CORRELATIONID, "TestCorrelationId"));
        }
    }
    

    @DisplayName("Sink DynamicDestinationMessageProcessor tests")
    @Nested
    @TestInstance(Lifecycle.PER_CLASS)
    class SinkDynamicDestinationMessageProcessorMessageProcessorTests {
        
        String topics[] = {SOL_ROOT_TOPIC+"/TestTopic1/SubTopic", SOL_ROOT_TOPIC+"/TestTopic2/SubTopic"};
        
        @BeforeAll
        void setUp() {
            Properties prop = new Properties();
            prop.setProperty("sol.record_processor_class", "com.solace.sink.connector.recordprocessor.SolDynamicDestinationRecordProcessor");
            prop.setProperty("sol.dynamic_destination", "true");
            prop.setProperty("sol.topics", String.join(", ", topics));
            prop.setProperty("sol.queue", SOL_QUEUE);
            connectorDeployment.startConnector(prop);
        }


        @DisplayName("TextMessage-DynamicDestinationMessageProcessor-start")
        @Test
        void kafkaConsumerTextMessageToTopicTest() {
            messageToKafkaTest(
                            // expected list of delivery queue and topics
                            null, new String[] {"ctrl/bus/1234/start"},
                            // kafka key and value
                            "ignore", "1234:start",
                            // additional checks with expected values
                            ImmutableMap.of(AdditionalCheck.ATTACHMENTBYTEBUFFER, "start"));
        }

        @DisplayName("TextMessage-DynamicDestinationMessageProcessor-stop")
        @Test
        void kafkaConsumerTextMessageToTopicTest2() {
            messageToKafkaTest(
                            // expected list of delivery queue and topics
                            null, new String[] {"ctrl/bus/1234/stop"},
                            // kafka key and value
                            "ignore", "1234:stop",
                            // additional checks with expected values
                            ImmutableMap.of(AdditionalCheck.ATTACHMENTBYTEBUFFER, "stop"));
        }

        @DisplayName("TextMessage-DynamicDestinationMessageProcessor-other")
        @Test
        void kafkaConsumerTextMessageToTopicTest3() {
            messageToKafkaTest(
                            // expected list of delivery queue and topics
                            null, new String[] {"comms/bus/1234"},
                            // kafka key and value
                            "ignore", "1234:other",
                            // additional checks with expected values
                            ImmutableMap.of(AdditionalCheck.ATTACHMENTBYTEBUFFER, "other"));
        }
    }

}
