package com.solace.connector.kafka.connect.sink.it;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.solace.connector.kafka.connect.sink.SolaceSinkConstants;
import com.solace.connector.kafka.connect.sink.it.util.ThrowingFunction;
import com.solace.connector.kafka.connect.sink.it.util.extensions.KafkaArgumentsProvider;
import com.solace.connector.kafka.connect.sink.it.util.extensions.KafkaArgumentsProvider.KafkaContext;
import com.solace.connector.kafka.connect.sink.it.util.extensions.KafkaArgumentsProvider.KafkaSource;
import com.solace.connector.kafka.connect.sink.it.util.extensions.pubsubplus.NetworkPubSubPlusContainerProvider;
import com.solace.connector.kafka.connect.sink.recordprocessor.SolDynamicDestinationRecordProcessor;
import com.solace.test.integration.junit.jupiter.extension.ExecutorServiceExtension;
import com.solace.test.integration.junit.jupiter.extension.ExecutorServiceExtension.ExecSvc;
import com.solace.test.integration.junit.jupiter.extension.PubSubPlusExtension;
import com.solace.test.integration.junit.jupiter.extension.PubSubPlusExtension.JCSMPProxy;
import com.solace.test.integration.junit.jupiter.extension.PubSubPlusExtension.ToxiproxyContext;
import com.solace.test.integration.semp.v2.SempV2Api;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnQueue;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnQueueSubscription;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.SDTException;
import com.solacesystems.jcsmp.SDTMap;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import eu.rekawek.toxiproxy.model.toxic.Latency;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.WaitingConsumer;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith(ExecutorServiceExtension.class)
@ExtendWith(PubSubPlusExtension.class)
@ExtendWith(KafkaArgumentsProvider.AutoDeleteSolaceConnectorDeploymentAfterEach.class)
public class SinkConnectorIT implements TestConstants {

    static Logger logger = LoggerFactory.getLogger(SinkConnectorIT.class);
    static TestSolaceQueueConsumer solaceQueueConsumer;
    static TestSolaceTopicConsumer solaceTopicConsumer;
    // Used to request additional verification types
    enum AdditionalCheck { ATTACHMENTBYTEBUFFER, CORRELATIONID }

    private Properties connectorProps;

    ////////////////////////////////////////////////////
    // Main setup/teardown

    @BeforeAll
    static void setUp(JCSMPSession jcsmpSession) throws JCSMPException {
        // Start consumer
        // Ensure test queue exists on PubSub+
        solaceTopicConsumer = new TestSolaceTopicConsumer(jcsmpSession); //TODO make this dynamic for concurrency
        solaceTopicConsumer.start();
        solaceQueueConsumer = new TestSolaceQueueConsumer(jcsmpSession); //TODO make this dynamic for concurrency
        solaceQueueConsumer.provisionQueue(SOL_QUEUE);
        solaceQueueConsumer.start();
    }

    @BeforeEach
    public void beforeEach(JCSMPProperties jcsmpProperties) {
        connectorProps = new Properties();
        connectorProps.setProperty(SolaceSinkConstants.SOL_HOST, String.format("tcp://%s:55555", NetworkPubSubPlusContainerProvider.DOCKER_NET_PUBSUB_ALIAS));
        connectorProps.setProperty(SolaceSinkConstants.SOL_USERNAME, jcsmpProperties.getStringProperty(JCSMPProperties.USERNAME));
        connectorProps.setProperty(SolaceSinkConstants.SOL_PASSWORD, jcsmpProperties.getStringProperty(JCSMPProperties.PASSWORD));
        connectorProps.setProperty(SolaceSinkConstants.SOL_VPN_NAME, jcsmpProperties.getStringProperty(JCSMPProperties.VPN_NAME));
    }

    @AfterAll
    static void cleanUp() {
        solaceTopicConsumer.close();
        solaceQueueConsumer.close();
    }



    ////////////////////////////////////////////////////
    // Test types

    void messageToKafkaTest(KafkaContext kafkaContext,
                            String expectedSolaceQueue,
                            String[] expectedSolaceTopics,
                            String kafkaKey,
                            String kafkaValue,
                            Map<AdditionalCheck, String> additionalChecks) {
        try {
            clearReceivedMessages();

            RecordMetadata metadata = sendMessagetoKafka(kafkaContext.getProducer(), kafkaKey, kafkaValue);
            assertMessageReceived(kafkaContext, expectedSolaceQueue, expectedSolaceTopics, metadata, additionalChecks);
        } catch (InterruptedException | SDTException e) {
            e.printStackTrace();
        }
    }

    void clearReceivedMessages() {
        // Clean catch queues first
        // TODO: fix possible concurrency issue with cleaning/wring the queue later
        TestSolaceQueueConsumer.solaceReceivedQueueMessages.clear();
        TestSolaceTopicConsumer.solaceReceivedTopicMessages.clear();
    }

    RecordMetadata sendMessagetoKafka(TestKafkaProducer producer, String kafkaKey, String kafkaValue) {
        // Send Kafka message
        RecordMetadata metadata = producer.sendMessageToKafka(kafkaKey, kafkaValue);
        assertNotNull(metadata);
        return metadata;
    }

    List<BytesXMLMessage> assertMessageReceived(KafkaContext kafkaContext,
                                                String expectedSolaceQueue,
                                                String[] expectedSolaceTopics,
                                                RecordMetadata metadata,
                                                Map<AdditionalCheck, String> additionalChecks)
            throws SDTException, InterruptedException {
        assertTimeoutPreemptively(Duration.ofMinutes(5), () -> {
            boolean isCommitted;
            do {
                Stream<String> groupIds = kafkaContext.getAdminClient().listConsumerGroups().all().get().stream()
                        .map(ConsumerGroupListing::groupId);

                Stream<Map.Entry<TopicPartition, OffsetAndMetadata>> partitionsToOffsets = groupIds
                        .map(groupId -> kafkaContext.getAdminClient().listConsumerGroupOffsets(groupId))
                        .map(ListConsumerGroupOffsetsResult::partitionsToOffsetAndMetadata)
                        .map((ThrowingFunction<KafkaFuture<Map<TopicPartition, OffsetAndMetadata>>,
                                Map<TopicPartition, OffsetAndMetadata>>) KafkaFuture::get)
                        .map(Map::entrySet)
                        .flatMap(Collection::stream);

                Long partitionOffset = partitionsToOffsets
                        .filter(e -> e.getKey().topic().equals(metadata.topic()))
                        .filter(e -> e.getKey().partition() == metadata.partition())
                        .map(Map.Entry::getValue)
                        .map(OffsetAndMetadata::offset)
                        .findAny()
                        .orElse(null);

                isCommitted = partitionOffset != null && partitionOffset >= metadata.offset();

                if (!isCommitted) {
                    logger.info("Waiting for record {} to be committed. Partition offset: {}", metadata, partitionOffset);
                    Thread.sleep(TimeUnit.SECONDS.toMillis(1));
                }
            } while (!isCommitted);
            logger.info("Record {} was committed", metadata);
        });

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

        return receivedMessages;
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
            connectorProps.setProperty("sol.record_processor_class", "com.solace.connector.kafka.connect.sink.recordprocessor.SolSimpleRecordProcessor");
            connectorProps.setProperty("sol.dynamic_destination", "false");
            connectorProps.setProperty("sol.topics", String.join(", ", topics));
            connectorProps.setProperty("sol.queue", SOL_QUEUE);
        }


        @DisplayName("TextMessage-QueueAndTopics-SolSampleSimpleMessageProcessor")
        @ParameterizedTest
        @ArgumentsSource(KafkaArgumentsProvider.class)
        void kafkaConsumerTextMessageToTopicTest(KafkaContext kafkaContext) {
            kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
            messageToKafkaTest(kafkaContext, SOL_QUEUE, topics,
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
            connectorProps.setProperty("sol.record_processor_class", "com.solace.connector.kafka.connect.sink.recordprocessor.SolSimpleKeyedRecordProcessor");
            connectorProps.setProperty("sol.dynamic_destination", "false");
            connectorProps.setProperty("sol.topics", String.join(", ", topics));
            connectorProps.setProperty("sol.kafka_message_key", "NONE");
            connectorProps.setProperty("sol.queue", SOL_QUEUE);
        }


        @DisplayName("TextMessage-QueueAndTopics-KeyedMessageProcessor-NONE")
        @ParameterizedTest
        @ArgumentsSource(KafkaArgumentsProvider.class)
        void kafkaConsumerTextMessageToTopicTest(KafkaContext kafkaContext) {
            kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
            messageToKafkaTest(kafkaContext, SOL_QUEUE, topics,
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
            connectorProps.setProperty("sol.record_processor_class", "com.solace.connector.kafka.connect.sink.recordprocessor.SolSimpleKeyedRecordProcessor");
            connectorProps.setProperty("sol.dynamic_destination", "false");
            connectorProps.setProperty("sol.topics", String.join(", ", topics));
            connectorProps.setProperty("sol.kafka_message_key", "DESTINATION");
            connectorProps.setProperty("sol.queue", SOL_QUEUE);
        }


        @DisplayName("TextMessage-QueueAndTopics-KeyedMessageProcessor-DESTINATION")
        @ParameterizedTest
        @ArgumentsSource(KafkaArgumentsProvider.class)
        void kafkaConsumerTextMessageToTopicTest(KafkaContext kafkaContext) {
            kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
            messageToKafkaTest(kafkaContext, SOL_QUEUE, topics,
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
            connectorProps.setProperty("sol.record_processor_class", "com.solace.connector.kafka.connect.sink.recordprocessor.SolSimpleKeyedRecordProcessor");
            connectorProps.setProperty("sol.dynamic_destination", "false");
            connectorProps.setProperty("sol.topics", String.join(", ", topics));
            connectorProps.setProperty("sol.kafka_message_key", "CORRELATION_ID");
            connectorProps.setProperty("sol.queue", SOL_QUEUE);
        }


        @DisplayName("TextMessage-QueueAndTopics-KeyedMessageProcessor-CORRELATION_ID")
        @ParameterizedTest
        @ArgumentsSource(KafkaArgumentsProvider.class)
        void kafkaConsumerTextMessageToTopicTest(KafkaContext kafkaContext) {
            kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
            messageToKafkaTest(kafkaContext, SOL_QUEUE, topics,
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
            connectorProps.setProperty("sol.record_processor_class", "com.solace.connector.kafka.connect.sink.recordprocessor.SolSimpleKeyedRecordProcessor");
            connectorProps.setProperty("sol.dynamic_destination", "false");
            connectorProps.setProperty("sol.topics", String.join(", ", topics));
            connectorProps.setProperty("sol.kafka_message_key", "CORRELATION_ID_AS_BYTES");
            connectorProps.setProperty("sol.queue", SOL_QUEUE);
        }


        @DisplayName("TextMessage-QueueAndTopics-KeyedMessageProcessor-CORRELATION_ID_AS_BYTES")
        @ParameterizedTest
        @ArgumentsSource(KafkaArgumentsProvider.class)
        void kafkaConsumerTextMessageToTopicTest(KafkaContext kafkaContext) {
            kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
            messageToKafkaTest(kafkaContext, SOL_QUEUE, topics,
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
            connectorProps.setProperty("sol.record_processor_class", SolDynamicDestinationRecordProcessor.class.getName());
            connectorProps.setProperty("sol.dynamic_destination", "true");
            connectorProps.setProperty("sol.topics", String.join(", ", topics));
            connectorProps.setProperty("sol.queue", SOL_QUEUE);
        }


        @DisplayName("TextMessage-DynamicDestinationMessageProcessor-start")
        @CartesianTest(name = "[{index}] transacted={0}, autoFlush={1}, kafka={2}")
        void kafkaConsumerTextMessageToTopicTest(@Values(booleans = { true, false }) boolean transacted,
                                                 @Values(booleans = { true, false }) boolean autoFlush,
                                                 @KafkaSource KafkaContext kafkaContext) {
            connectorProps.setProperty(SolaceSinkConstants.SOl_USE_TRANSACTIONS_FOR_TOPICS, Boolean.toString(transacted));
            connectorProps.setProperty(SolaceSinkConstants.SOL_AUTOFLUSH_SIZE, Integer.toString(autoFlush ? 1 : 200));
            kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
            messageToKafkaTest(kafkaContext,
                            // expected list of delivery queue and topics
                            null, new String[] {"ctrl/bus/1234/start"},
                            // kafka key and value
                            "ignore", "1234:start",
                            // additional checks with expected values
                            ImmutableMap.of(AdditionalCheck.ATTACHMENTBYTEBUFFER, "start"));
        }

        @DisplayName("TextMessage-DynamicDestinationMessageProcessor-stop")
        @CartesianTest(name = "[{index}] transacted={0}, autoFlush={1}, kafka={2}")
        void kafkaConsumerTextMessageToTopicTest2(@Values(booleans = { true, false }) boolean transacted,
                                                  @Values(booleans = { true, false }) boolean autoFlush,
                                                  @KafkaSource KafkaContext kafkaContext) {
            connectorProps.setProperty(SolaceSinkConstants.SOl_USE_TRANSACTIONS_FOR_TOPICS, Boolean.toString(transacted));
            connectorProps.setProperty(SolaceSinkConstants.SOL_AUTOFLUSH_SIZE, Integer.toString(autoFlush ? 1 : 200));
            kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
            messageToKafkaTest(kafkaContext,
                            // expected list of delivery queue and topics
                            null, new String[] {"ctrl/bus/1234/stop"},
                            // kafka key and value
                            "ignore", "1234:stop",
                            // additional checks with expected values
                            ImmutableMap.of(AdditionalCheck.ATTACHMENTBYTEBUFFER, "stop"));
        }

        @DisplayName("TextMessage-DynamicDestinationMessageProcessor-other")
        @CartesianTest(name = "[{index}] transacted={0}, autoFlush={1}, kafka={2}")
        void kafkaConsumerTextMessageToTopicTest3(@Values(booleans = { true, false }) boolean transacted,
                                                  @Values(booleans = { true, false }) boolean autoFlush,
                                                  @KafkaSource KafkaContext kafkaContext) {
            connectorProps.setProperty(SolaceSinkConstants.SOl_USE_TRANSACTIONS_FOR_TOPICS, Boolean.toString(transacted));
            connectorProps.setProperty(SolaceSinkConstants.SOL_AUTOFLUSH_SIZE, Integer.toString(autoFlush ? 1 : 200));
            kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
            messageToKafkaTest(kafkaContext,
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

        @ParameterizedTest
        @ArgumentsSource(KafkaArgumentsProvider.class)
        void testFailPubSubConnection(KafkaContext kafkaContext) {
            connectorProps.setProperty("sol.vpn_name", randomAlphanumeric(10));
            kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps, true);
            AtomicReference<JsonObject> connectorStatus = new AtomicReference<>(new JsonObject());
            assertTimeoutPreemptively(Duration.ofMinutes(1), () -> {
                JsonObject taskStatus;
                do {
                    JsonObject status = kafkaContext.getSolaceConnectorDeployment().getConnectorStatus();
                    connectorStatus.set(status);
                    taskStatus = status.getAsJsonArray("tasks").get(0).getAsJsonObject();
                } while (!taskStatus.get("state").getAsString().equals("FAILED"));
                assertThat(taskStatus.get("trace").getAsString(), containsString("Message VPN Not Allowed"));
            }, () -> "Timed out waiting for connector to fail: " + GSON.toJson(connectorStatus.get()));
        }

        @CartesianTest(name = "[{index}] dynamicDestination={0}, autoFlush={1}, kafka={2}")
        void testRebalancedKafkaConsumers(@Values(booleans = { false, true }) boolean dynamicDestination,
                                          @Values(booleans = { false, true }) boolean autoFlush,
                                          @KafkaSource KafkaContext kafkaContext,
                                          @JCSMPProxy ToxiproxyContext jcsmpProxyContext,
                                          @ExecSvc(poolSize = 2, scheduled = true) ScheduledExecutorService executorService)
                throws Exception {
            String topicName;

            if (dynamicDestination) {
                topicName = "comms/bus/" + RandomStringUtils.randomAlphanumeric(4);
                connectorProps.setProperty(SolaceSinkConstants.SOL_DYNAMIC_DESTINATION, Boolean.toString(true));
                connectorProps.setProperty(SolaceSinkConstants.SOL_RECORD_PROCESSOR, SolDynamicDestinationRecordProcessor.class.getName());
                connectorProps.setProperty(SolaceSinkConstants.SOL_AUTOFLUSH_SIZE, Integer.toString(autoFlush ? 1 : 100));
            } else {
                topicName = SOL_ROOT_TOPIC + "/" + RandomStringUtils.randomAlphanumeric(100);
                connectorProps.setProperty(SolaceSinkConstants.SOl_QUEUE, SOL_QUEUE);
                connectorProps.setProperty(SolaceSinkConstants.SOL_TOPICS, topicName);
                connectorProps.setProperty(SolaceSinkConstants.SOL_AUTOFLUSH_SIZE, Integer.toString(autoFlush ? 2 : 100));
            }

            connectorProps.setProperty(SolaceSinkConstants.SOL_HOST, String.format("tcp://%s:%s",
                    jcsmpProxyContext.getDockerNetworkAlias(), jcsmpProxyContext.getProxy().getOriginalProxyPort()));
            connectorProps.setProperty(SolaceSinkConstants.SOl_USE_TRANSACTIONS_FOR_QUEUE, Boolean.toString(true));
            connectorProps.setProperty(SolaceSinkConstants.SOl_USE_TRANSACTIONS_FOR_TOPICS, Boolean.toString(true));
            connectorProps.setProperty(SolaceSinkConstants.SOL_CHANNEL_PROPERTY_reconnectRetries, Integer.toString(-1));
            connectorProps.setProperty("errors.retry.timeout", Long.toString(-1));
            connectorProps.setProperty("consumer.override.max.poll.interval.ms", "1000");

            kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);

            Latency lag = jcsmpProxyContext.getProxy().toxics()
                    .latency("lag", ToxicDirection.UPSTREAM, TimeUnit.DAYS.toMillis(1));

            clearReceivedMessages();
            String recordValue = randomAlphanumeric(100);
            Future<RecordMetadata> recordMetadataFuture = executorService.schedule(() ->
                            sendMessagetoKafka(kafkaContext.getProducer(), randomAlphanumeric(100),
                                    dynamicDestination ?
                                            topicName.substring("comms/bus/".length()) + ":" + recordValue :
                                            recordValue),
                    5, TimeUnit.SECONDS);

            logger.info("Waiting for Kafka poll interval to expire...");
            WaitingConsumer logConsumer = new WaitingConsumer();
            kafkaContext.getConnection().getConnectContainer().followOutput(logConsumer);
            logConsumer.waitUntil(frame -> frame.getUtf8String().contains("consumer poll timeout has expired"),
                    1, TimeUnit.MINUTES);
            logConsumer.waitUntil(frame -> frame.getUtf8String().contains("Connection attempt failed to host"),
                    1, TimeUnit.MINUTES);
            Thread.sleep(TimeUnit.SECONDS.toMillis(5));

            assertEquals("RUNNING",
                    kafkaContext.getSolaceConnectorDeployment().getConnectorStatus()
                            .getAsJsonArray("tasks").get(0).getAsJsonObject().get("state").getAsString());

            logger.info("Removing toxic {}", lag.getName());
            lag.remove();

            logger.info("Checking consumption of message");
            assertMessageReceived(kafkaContext, dynamicDestination ? null : SOL_QUEUE,
                    new String[]{topicName},
                    recordMetadataFuture.get(30, TimeUnit.SECONDS),
                    ImmutableMap.of(AdditionalCheck.ATTACHMENTBYTEBUFFER, recordValue));

            logger.info("Sending a new message to verify that Solace producers & Kafka consumers have recovered");
            clearReceivedMessages();
            String newRecordValue = randomAlphanumeric(100);
            messageToKafkaTest(kafkaContext, dynamicDestination ? null : SOL_QUEUE,
                    new String[]{topicName},
                    randomAlphanumeric(100), dynamicDestination ?
                            topicName.substring("comms/bus/".length()) + ":" + newRecordValue :
                            newRecordValue,
                    ImmutableMap.of(AdditionalCheck.ATTACHMENTBYTEBUFFER, newRecordValue));
        }

        @Disabled()
        @CartesianTest(name = "[{index}] dynamicDestination={0}, autoFlush={1}, kafka={2}")
        void testCommitRollback(@Values(booleans = { false, true }) boolean dynamicDestination,
                                @Values(booleans = { false, true }) boolean autoFlush,
                                @KafkaSource KafkaContext kafkaContext,
                                JCSMPSession jcsmpSession,
                                SempV2Api sempV2Api,
                                Queue queue,
                                @ExecSvc(poolSize = 2, scheduled = true) ScheduledExecutorService executorService)
                throws Exception {
            String topicName;
            if (dynamicDestination) {
                topicName = "comms/bus/" + RandomStringUtils.randomAlphanumeric(4);
                connectorProps.setProperty(SolaceSinkConstants.SOL_DYNAMIC_DESTINATION, Boolean.toString(true));
                connectorProps.setProperty(SolaceSinkConstants.SOL_RECORD_PROCESSOR, SolDynamicDestinationRecordProcessor.class.getName());
                connectorProps.setProperty(SolaceSinkConstants.SOL_AUTOFLUSH_SIZE, Integer.toString(autoFlush ? 1 : 200));
            } else {
                topicName = RandomStringUtils.randomAlphanumeric(100);
                connectorProps.setProperty(SolaceSinkConstants.SOl_QUEUE, queue.getName());
                connectorProps.setProperty(SolaceSinkConstants.SOL_TOPICS, topicName);
                connectorProps.setProperty(SolaceSinkConstants.SOL_AUTOFLUSH_SIZE, Integer.toString(autoFlush ? 2 : 200));
            }

            connectorProps.setProperty(SolaceSinkConstants.SOl_USE_TRANSACTIONS_FOR_QUEUE, Boolean.toString(true));
            connectorProps.setProperty(SolaceSinkConstants.SOl_USE_TRANSACTIONS_FOR_TOPICS, Boolean.toString(true));
            connectorProps.setProperty("errors.retry.timeout", Long.toString(-1));

            sempV2Api.config().updateMsgVpnQueue(SOL_VPN, queue.getName(), new ConfigMsgVpnQueue().maxMsgSize(1),
                    null, null);
            sempV2Api.config().createMsgVpnQueueSubscription(SOL_VPN, queue.getName(),
                    new ConfigMsgVpnQueueSubscription().subscriptionTopic(topicName), null, null);
            assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
                while (sempV2Api.monitor().getMsgVpnQueue(SOL_VPN, queue.getName(), null).getData()
                        .getMaxMsgSize() != 1) {
                    logger.info("Waiting for queue {} to have max message size of 1", queue.getName());
                    Thread.sleep(100);
                }
            });

            try (TestSolaceQueueConsumer solaceConsumer1 = new TestSolaceQueueConsumer(jcsmpSession)) {
                solaceConsumer1.setQueueName(queue.getName());
                solaceConsumer1.start();

                kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);

                clearReceivedMessages();
                String recordValue = randomAlphanumeric(100);
                Future<RecordMetadata> recordMetadataFuture = executorService.schedule(() ->
                        sendMessagetoKafka(kafkaContext.getProducer(), randomAlphanumeric(100),
                                dynamicDestination ?
                                        topicName.substring("comms/bus/".length()) + ":" + recordValue :
                                        recordValue),
                        5, TimeUnit.SECONDS);

                WaitingConsumer logConsumer = new WaitingConsumer();
                kafkaContext.getConnection().getConnectContainer().followOutput(logConsumer);
                logConsumer.waitUntil(frame -> frame.getUtf8String()
                        .contains("Document Is Too Large"), 30, TimeUnit.SECONDS);
                if (autoFlush) {
                    logConsumer.waitUntil(frame -> frame.getUtf8String()
                            .contains("ConnectException from SinkTask"), 30, TimeUnit.SECONDS);
                } else {
                    logConsumer.waitUntil(frame -> frame.getUtf8String()
                            .contains("Offset commit failed, rewinding to last committed offsets"), 1, TimeUnit.MINUTES);
                }
                Thread.sleep(5000);

                assertEquals("RUNNING", kafkaContext.getSolaceConnectorDeployment().getConnectorStatus()
                        .getAsJsonArray("tasks").get(0).getAsJsonObject().get("state").getAsString());

                executorService.schedule(() -> {
                    logger.info("Restoring max message size for queue {}", queue.getName());
                    return sempV2Api.config().updateMsgVpnQueue(SOL_VPN, queue.getName(),
                            new ConfigMsgVpnQueue().maxMsgSize(10000000), null, null);
                }, 5, TimeUnit.SECONDS);

                logger.info("Waiting for Solace transaction to be committed");
                logConsumer.waitUntil(frame -> frame.getUtf8String()
                        .contains("Committed Solace records for transaction"), 5, TimeUnit.MINUTES);

                logger.info("Sending another message to Kafka since Kafka Connect won't commit existing messages " +
                        "after a retry until a new message is received");
                sendMessagetoKafka(kafkaContext.getProducer(), randomAlphanumeric(100), randomAlphanumeric(100));

                List<Destination> receivedMsgDestinations = new ArrayList<>();
                while (receivedMsgDestinations.size() < (dynamicDestination ? 1 : 2)) {
                    logger.info("Checking consumption of messages");
                    receivedMsgDestinations.addAll(assertMessageReceived(kafkaContext, queue.getName(), new String[0],
                            recordMetadataFuture.get(30, TimeUnit.SECONDS),
                            ImmutableMap.of(AdditionalCheck.ATTACHMENTBYTEBUFFER, recordValue))
                            .stream()
                            .map(BytesXMLMessage::getDestination)
                            .collect(Collectors.toList()));
                }

                if (dynamicDestination) {
                    assertThat(receivedMsgDestinations, hasSize(1));
                    assertThat(receivedMsgDestinations, hasItems(JCSMPFactory.onlyInstance().createTopic(topicName)));
                } else {
                    assertThat(receivedMsgDestinations, hasSize(2));
                    assertThat(receivedMsgDestinations, hasItems(queue,
                            JCSMPFactory.onlyInstance().createTopic(topicName)));
                }
            }
        }
    }

}
