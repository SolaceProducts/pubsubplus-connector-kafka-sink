package com.solace.connector.kafka.connect.sink.it;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class TestKafkaProducer  implements AutoCloseable {

    static Logger logger = LoggerFactory.getLogger(TestKafkaProducer.class.getName());
    private final String bootstrapHost;
    private final String kafkaTopic;
    private KafkaProducer<byte[], byte[]> producer;

    public TestKafkaProducer(String bootstrapHost, String kafkaTestTopic) {
        this.bootstrapHost = bootstrapHost;
        this.kafkaTopic = kafkaTestTopic;
    }

    public void start() {
        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapHost);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        // create safe Producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");

        // high throughput producer (at the expense of a bit of latency and CPU usage)
//        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
//        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
//        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // 32 KB batch size

        // create the producer
        producer = new KafkaProducer<byte[], byte[]>(properties);
    }

    public RecordMetadata sendMessageToKafka(String msgKey, String msgValue) {
        assert(msgValue != null);
        RecordMetadata recordmetadata = null;
        try {
            recordmetadata = producer.send(new ProducerRecord<>(kafkaTopic, msgKey.getBytes(), msgValue.getBytes())).get();
            logger.info("Message sent to Kafka topic " + kafkaTopic);
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Failed to send message to Kafka topic " + kafkaTopic, e);
        }
        return recordmetadata;
    }

    @Override
    public void close() {
        producer.close();
    }
}
