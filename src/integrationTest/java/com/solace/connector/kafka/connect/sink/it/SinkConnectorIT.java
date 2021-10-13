package com.solace.connector.kafka.connect.sink.it;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.solace.connector.kafka.connect.sink.SolaceSinkConstants;
import com.solace.connector.kafka.connect.sink.it.util.KafkaConnection;
import com.solace.connector.kafka.connect.sink.it.util.extensions.NetworkPubSubPlusExtension;
import com.solace.connector.kafka.connect.sink.it.util.testcontainers.BitnamiKafkaConnectContainer;
import com.solace.connector.kafka.connect.sink.it.util.testcontainers.ConfluentKafkaConnectContainer;
import com.solace.connector.kafka.connect.sink.it.util.testcontainers.ConfluentKafkaControlCenterContainer;
import com.solace.connector.kafka.connect.sink.it.util.testcontainers.ConfluentKafkaSchemaRegistryContainer;
import com.solace.test.integration.junit.jupiter.extension.ExecutorServiceExtension;
import com.solace.test.integration.junit.jupiter.extension.ExecutorServiceExtension.ExecSvc;
import com.solace.test.integration.semp.v2.SempV2Api;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnQueue;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.SDTException;
import com.solacesystems.jcsmp.SDTMap;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.WaitingConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.fail;

@Testcontainers
@ExtendWith(ExecutorServiceExtension.class)
public class SinkConnectorIT implements TestConstants {

    static Logger logger = LoggerFactory.getLogger(SinkConnectorIT.class.getName());
    // Connectordeployment creates a Kafka topic "kafkaTestTopic", which is used next
    static KafkaConnection kafkaConnection;
    static SolaceConnectorDeployment connectorDeployment;
    static TestKafkaProducer kafkaProducer;
    static TestSolaceQueueConsumer solaceQueueConsumer;
    static TestSolaceTopicConsumer solaceTopicConsumer;
    // Used to request additional verification types
    static enum AdditionalCheck { ATTACHMENTBYTEBUFFER, CORRELATIONID }

    @RegisterExtension
    public static final NetworkPubSubPlusExtension PUB_SUB_PLUS_EXTENSION = new NetworkPubSubPlusExtension();

    @Container
    public static final BitnamiKafkaConnectContainer BITNAMI_KAFKA_CONTAINER = new BitnamiKafkaConnectContainer()
            .withNetwork(PUB_SUB_PLUS_EXTENSION.getDockerNetwork());

//    @Container
//    public static final KafkaContainer CP_KAFKA_CONTAINER = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka").withTag("6.2.1"))
//            .withNetwork(PUB_SUB_PLUS_EXTENSION.getDockerNetwork())
//            .withNetworkAliases("kafka");
//
//    @Container
//    public static final ConfluentKafkaSchemaRegistryContainer CP_SCHEMA_REGISTRY_CONTAINER = new ConfluentKafkaSchemaRegistryContainer(CP_KAFKA_CONTAINER)
//            .withNetworkAliases("schema-registry");
//
//    @Container
//    public static final ConfluentKafkaControlCenterContainer CP_CONTROL_CENTER_CONTAINER = new ConfluentKafkaControlCenterContainer(CP_KAFKA_CONTAINER, CP_SCHEMA_REGISTRY_CONTAINER);
//
//    @Container
//    public static final ConfluentKafkaConnectContainer CP_CONNECT_CONTAINER = new ConfluentKafkaConnectContainer(CP_KAFKA_CONTAINER, CP_SCHEMA_REGISTRY_CONTAINER);

    ////////////////////////////////////////////////////
    // Main setup/teardown

    @BeforeAll
    static void setUp(JCSMPSession jcsmpSession, JCSMPProperties jcsmpProperties) {
        kafkaConnection = new KafkaConnection(BITNAMI_KAFKA_CONTAINER.getBootstrapServers(), BITNAMI_KAFKA_CONTAINER.getConnectUrl(), BITNAMI_KAFKA_CONTAINER, BITNAMI_KAFKA_CONTAINER);
//        kafkaConnection = new KafkaConnection(CP_KAFKA_CONTAINER.getBootstrapServers(), CP_CONNECT_CONTAINER.getConnectUrl(), CP_KAFKA_CONTAINER, CP_CONNECT_CONTAINER);
        kafkaProducer = new TestKafkaProducer(kafkaConnection.getBootstrapServers(), SolaceConnectorDeployment.kafkaTestTopic);
        connectorDeployment = new SolaceConnectorDeployment(kafkaConnection,
                String.format("tcp://%s:55555", PUB_SUB_PLUS_EXTENSION.getNetworkAlias()), jcsmpProperties);
        try {
            connectorDeployment.waitForConnectorRestIFUp();
            connectorDeployment.startAdminClient();
            // Start consumer
            // Ensure test queue exists on PubSub+
            solaceTopicConsumer = new TestSolaceTopicConsumer(jcsmpSession);
            solaceTopicConsumer.start();
            solaceQueueConsumer = new TestSolaceQueueConsumer(jcsmpSession);
            solaceQueueConsumer.provisionQueue(SOL_QUEUE);
            solaceQueueConsumer.start();
            Thread.sleep(1000L);
        } catch (JCSMPException | InterruptedException e1) {
            e1.printStackTrace();
        }
    }

    @BeforeEach
    public void beforeEach() {
        connectorDeployment.provisionKafkaTestTopic();
        kafkaProducer.start();
    }

    @AfterAll
    static void cleanUp() {
        solaceTopicConsumer.close();
        solaceQueueConsumer.close();
        connectorDeployment.closeAdminClient();
    }

    @AfterEach
    public void afterEach() throws Exception {
        kafkaProducer.close();
        connectorDeployment.deleteKafkaTestTopic();
        connectorDeployment.deleteConnector();
    }



    ////////////////////////////////////////////////////
    // Test types

    void messageToKafkaTest(String expectedSolaceQueue, String[] expectedSolaceTopics, String kafkaKey, String kafkaValue,
                    Map<AdditionalCheck, String> additionalChecks) {
        try {
            clearReceivedMessages();

            RecordMetadata metadata = sendMessagetoKafka(kafkaKey, kafkaValue);
            assertMessageReceived(expectedSolaceQueue, expectedSolaceTopics, metadata, additionalChecks);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (SDTException e) {
            e.printStackTrace();
        }
    }

    void clearReceivedMessages() {
        // Clean catch queues first
        // TODO: fix possible concurrency issue with cleaning/wring the queue later
        TestSolaceQueueConsumer.solaceReceivedQueueMessages.clear();
        TestSolaceTopicConsumer.solaceReceivedTopicMessages.clear();
    }

    RecordMetadata sendMessagetoKafka(String kafkaKey, String kafkaValue) {
        // Send Kafka message
        RecordMetadata metadata = kafkaProducer.sendMessageToKafka(kafkaKey, kafkaValue);
        assertNotNull(metadata);
        return metadata;
    }

    void assertMessageReceived(String expectedSolaceQueue, String[] expectedSolaceTopics, RecordMetadata metadata,
                               Map<AdditionalCheck, String> additionalChecks) throws SDTException, InterruptedException {
        List<BytesXMLMessage> receivedMessages = new ArrayList<>();

        // Wait for PubSub+ to report messages - populate queue and topics if provided
        if (expectedSolaceQueue != null) {
            BytesXMLMessage queueMessage = TestSolaceQueueConsumer.solaceReceivedQueueMessages.poll(5,TimeUnit.MINUTES);
            assertNotNull(queueMessage);
            receivedMessages.add(queueMessage);
        } else {
            assert(TestSolaceQueueConsumer.solaceReceivedQueueMessages.size() == 0);
        }
        for(String s : expectedSolaceTopics) {
            BytesXMLMessage newTopicMessage = TestSolaceTopicConsumer.solaceReceivedTopicMessages.poll(5,TimeUnit.SECONDS);
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
            assertEquals(metadata.topic(), userHeader.getString("k_topic"));
            assertEquals(Long.toString(metadata.partition()), userHeader.getString("k_partition"));
            assertEquals(Long.toString(metadata.offset()), userHeader.getString("k_offset"));
            assertThat(message.getApplicationMessageType(), containsString(metadata.topic()));
            // additional checks as requested
            if (additionalChecks != null) {
                for (Map.Entry<AdditionalCheck, String> check : additionalChecks.entrySet()) {
                    if (check.getKey() == AdditionalCheck.ATTACHMENTBYTEBUFFER) {
                        // Verify contents of the message AttachmentByteBuffer
                        assertArrayEquals(check.getValue().getBytes(), message.getAttachmentByteBuffer().array());
                    }
                    if (check.getKey() == AdditionalCheck.CORRELATIONID) {
                        // Verify contents of the message correlationId
                        assertEquals(check.getValue(), message.getCorrelationId());
                    }
                }
            }
        }
    }

    ////////////////////////////////////////////////////
    // Scenarios

    @DisplayName("Sink SimpleMessageProcessor tests")
    @Nested
    @TestInstance(Lifecycle.PER_CLASS)
    class SinkConnectorSimpleMessageProcessorTests {

        String[] topics = {SOL_ROOT_TOPIC+"/TestTopic1/SubTopic", SOL_ROOT_TOPIC+"/TestTopic2/SubTopic"};

        @BeforeEach
        void setUp() {
            Properties prop = new Properties();
            prop.setProperty("sol.record_processor_class", "com.solace.connector.kafka.connect.sink.recordprocessor.SolSimpleRecordProcessor");
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

        String[] topics = {SOL_ROOT_TOPIC+"/TestTopic1/SubTopic", SOL_ROOT_TOPIC+"/TestTopic2/SubTopic"};

        @BeforeEach
        void setUp() {
            Properties prop = new Properties();
            prop.setProperty("sol.record_processor_class", "com.solace.connector.kafka.connect.sink.recordprocessor.SolSimpleKeyedRecordProcessor");
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

        String[] topics = {SOL_ROOT_TOPIC+"/TestTopic1/SubTopic", SOL_ROOT_TOPIC+"/TestTopic2/SubTopic"};

        @BeforeEach
        void setUp() {
            Properties prop = new Properties();
            prop.setProperty("sol.record_processor_class", "com.solace.connector.kafka.connect.sink.recordprocessor.SolSimpleKeyedRecordProcessor");
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

        String[] topics = {SOL_ROOT_TOPIC+"/TestTopic1/SubTopic", SOL_ROOT_TOPIC+"/TestTopic2/SubTopic"};

        @BeforeEach
        void setUp() {
            Properties prop = new Properties();
            prop.setProperty("sol.record_processor_class", "com.solace.connector.kafka.connect.sink.recordprocessor.SolSimpleKeyedRecordProcessor");
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

        String[] topics = {SOL_ROOT_TOPIC+"/TestTopic1/SubTopic", SOL_ROOT_TOPIC+"/TestTopic2/SubTopic"};

        @BeforeEach
        void setUp() {
            Properties prop = new Properties();
            prop.setProperty("sol.record_processor_class", "com.solace.connector.kafka.connect.sink.recordprocessor.SolSimpleKeyedRecordProcessor");
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

        String[] topics = {SOL_ROOT_TOPIC+"/TestTopic1/SubTopic", SOL_ROOT_TOPIC+"/TestTopic2/SubTopic"};

        @BeforeEach
        void setUp() {
            Properties prop = new Properties();
            prop.setProperty("sol.record_processor_class", "com.solace.connector.kafka.connect.sink.recordprocessor.SolDynamicDestinationRecordProcessor");
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

    @DisplayName("Solace connector lifecycle tests")
    @Nested
    @TestInstance(Lifecycle.PER_CLASS)
    class SolaceConnectorLifecycleTests {
        private final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

        @Test
        void testFailPubSubConnection() {
            Properties prop = new Properties();
            prop.setProperty("sol.vpn_name", randomAlphanumeric(10));
            connectorDeployment.startConnector(prop, true);
            AtomicReference<JsonObject> connectorStatus = new AtomicReference<>(new JsonObject());
            assertTimeoutPreemptively(Duration.ofMinutes(1), () -> {
                JsonObject taskStatus;
                do {
                    JsonObject status = connectorDeployment.getConnectorStatus();
                    connectorStatus.set(status);
                    taskStatus = status.getAsJsonArray("tasks").get(0).getAsJsonObject();
                } while (!taskStatus.get("state").getAsString().equals("FAILED"));
                assertThat(taskStatus.get("trace").getAsString(), containsString("Message VPN Not Allowed"));
            }, () -> "Timed out waiting for connector to fail: " + GSON.toJson(connectorStatus.get()));
        }

        @ParameterizedTest(name = "[{index}] autoFlush={0}")
        @ValueSource(booleans = { true, false })
        void testCommitRollback(boolean autoFlush, JCSMPSession jcsmpSession, SempV2Api sempV2Api,
                                @ExecSvc(poolSize = 2, scheduled = true) ScheduledExecutorService executorService)
                throws Exception {
            Queue queue = JCSMPFactory.onlyInstance().createQueue(randomAlphanumeric(100));

            try (TestSolaceQueueConsumer solaceConsumer1 = new TestSolaceQueueConsumer(jcsmpSession)) {
                EndpointProperties endpointProperties = new EndpointProperties();
                endpointProperties.setMaxMsgSize(1);
                solaceConsumer1.provisionQueue(queue.getName(), endpointProperties);
                solaceConsumer1.start();

                Properties prop = new Properties();
                prop.setProperty(SolaceSinkConstants.SOl_QUEUE, queue.getName());
                prop.setProperty(SolaceSinkConstants.SOL_QUEUE_MESSAGES_AUTOFLUSH_SIZE, Integer.toString(autoFlush ? 1 : 100));
                prop.setProperty(SolaceSinkConstants.SOl_USE_TRANSACTIONS_FOR_QUEUE, Boolean.toString(true));
                prop.setProperty("errors.retry.timeout", Long.toString(-1));
                connectorDeployment.startConnector(prop);

                clearReceivedMessages();
                String recordValue = randomAlphanumeric(100);
                Future<RecordMetadata> recordMetadata = executorService.schedule(() ->
                        sendMessagetoKafka(randomAlphanumeric(100), recordValue), 5, TimeUnit.SECONDS);

                WaitingConsumer logConsumer = new WaitingConsumer();
                kafkaConnection.getConnectContainer().followOutput(logConsumer);
                logConsumer.waitUntil(frame -> frame.getUtf8String()
                        .contains("Document Is Too Large"), 30, TimeUnit.SECONDS);
                if (autoFlush) {
                    logConsumer.waitUntil(frame -> frame.getUtf8String()
                            .contains("RetriableException from SinkTask"), 30, TimeUnit.SECONDS);
                } else {
                    logConsumer.waitUntil(frame -> frame.getUtf8String()
                            .contains("Offset commit failed, rewinding to last committed offsets"), 1, TimeUnit.MINUTES);
                }
                Thread.sleep(5000);

                Assertions.assertEquals("RUNNING",
                        connectorDeployment.getConnectorStatus().getAsJsonArray("tasks").get(0).getAsJsonObject().get("state").getAsString());

                sempV2Api.config().updateMsgVpnQueue(SOL_VPN, queue.getName(), new ConfigMsgVpnQueue().maxMsgSize(10000000), null);
                assertMessageReceived(queue.getName(), new String[0], recordMetadata.get(30, TimeUnit.SECONDS),
                        ImmutableMap.of(AdditionalCheck.ATTACHMENTBYTEBUFFER, recordValue));
            } finally {
                jcsmpSession.deprovision(queue, JCSMPSession.FLAG_IGNORE_DOES_NOT_EXIST);
            }
        }
    }

}
