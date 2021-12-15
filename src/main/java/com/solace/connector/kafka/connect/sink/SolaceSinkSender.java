/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.solace.connector.kafka.connect.sink;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.SDTException;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.Topic;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class SolaceSinkSender {
  private static final Logger log = LoggerFactory.getLogger(SolaceSinkSender.class);

  private final SolaceSinkConnectorConfig sconfig;
  private final SolSessionHandler sessionHandler;
  final SolProducerHandler producerHandler;
  private final List<Topic> topics = new ArrayList<>();
  private Queue solQueue = null;
  private final SolRecordProcessorIF processor;
  private final String kafkaKey;
  private final SolaceSinkTask sinkTask;
  private final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

  /**
   * Class that sends Solace Messages from Kafka Records.
   * @param sconfig JCSMP Configuration
   * @param sessionHandler SolSessionHandler
   * @param sinkTask Connector Sink Task
   * @throws JCSMPException
   */
  public SolaceSinkSender(final SolaceSinkConnectorConfig sconfig,
                          final SolSessionHandler sessionHandler,
                          final SolaceSinkTask sinkTask) throws JCSMPException {
    this.sconfig = sconfig;
    this.sessionHandler = sessionHandler;
    this.sinkTask = sinkTask;
    this.kafkaKey = sconfig.getString(SolaceSinkConstants.SOL_KAFKA_MESSAGE_KEY);
    this.producerHandler = new SolProducerHandler(sconfig, sessionHandler, this::txAutoFlushHandler);
    this.processor = sconfig.getConfiguredInstance(SolaceSinkConstants.SOL_RECORD_PROCESSOR, SolRecordProcessorIF.class);

    for (String topic : sconfig.getTopics()) {
      this.topics.add(JCSMPFactory.onlyInstance().createTopic(topic.trim()));
    }

    if (sconfig.getString(SolaceSinkConstants.SOl_QUEUE) != null) {
      solQueue = JCSMPFactory.onlyInstance().createQueue(sconfig.getString(SolaceSinkConstants.SOl_QUEUE));
    }
  }

  /**
   * Send Solace Message from Kafka Record.
   * @param record Kafka Records
   */
  public void sendRecord(SinkRecord record) {
    BytesXMLMessage message;
    try {
      message = processor.processRecord(kafkaKey, record);
      offsets.put(new TopicPartition(record.topic(), record.kafkaPartition()),
          new OffsetAndMetadata(record.kafkaOffset()));
      log.trace("================ Processed record details, topic: {}, Partition: {}, Offset: {}", record.topic(),
          record.kafkaPartition(), record.kafkaOffset());
    } catch (Exception e) {
      if (sconfig.getBoolean(SolaceSinkConstants.SOL_RECORD_PROCESSOR_IGNORE_ERROR)) {
        log.warn("================ Encountered exception in record processing for record of topic {}, partition {} " +
                        "and offset {}....discarded", record.topic(), record.kafkaPartition(), record.kafkaOffset(), e);
        return;
      } else {
        throw new ConnectException("Encountered exception in record processing", e);
      }
    }

    if (message.getAttachmentContentLength() == 0 || message.getAttachmentByteBuffer() == null) {
      log.info("================ Received record that had no data....discarded");
      return;
    }

    mayEnrichUserPropertiesWithKafkaRecordHeaders(record, message);

    if (sconfig.getBoolean(SolaceSinkConstants.SOL_DYNAMIC_DESTINATION)) {
      // Process use Dynamic destination from SolRecordProcessor
      SDTMap userMap = message.getProperties();
      Destination dest = null;
      try {
        dest = userMap.getDestination("dynamicDestination");
      } catch (SDTException e) {
        if (sconfig.getBoolean(SolaceSinkConstants.SOL_RECORD_PROCESSOR_IGNORE_ERROR)) {
          log.warn("================ Received exception retrieving Dynamic Destination....discarded", e);
          return;
        } else {
          throw new ConnectException("Received exception retrieving Dynamic Destination", e);
        }
      }
      try {
        producerHandler.send(message, dest);
      } catch (IllegalArgumentException e) {
        throw new ConnectException(String.format("Received exception while sending message to topic %s",
                dest != null ? dest.getName() : null), e);
      } catch (JCSMPException e) {
        throw new RetriableException(String.format("Received exception while sending message to topic %s",
                dest != null ? dest.getName() : null), e);
      }
    } else {
      // Process when Dynamic destination is not set
      if (solQueue != null) {
        try {
          producerHandler.send(message, solQueue);
        } catch (IllegalArgumentException e) {
          throw new ConnectException(String.format("Received exception while sending message to queue %s",
                  solQueue.getName()), e);
        } catch (JCSMPException e) {
          throw new RetriableException(String.format("Received exception while sending message to queue %s",
                  solQueue.getName()), e);
        }
      }
      if (topics.size() != 0 && message.getDestination() == null) {
        for (Topic topic : topics) {
          try {
            producerHandler.send(message, topic);
          } catch (IllegalArgumentException e) {
            throw new ConnectException(String.format("Received exception while sending message to topic %s",
                    topic.getName()), e);
          } catch (JCSMPException e) {
            throw new RetriableException(String.format("Received exception while sending message to topic %s",
                    topic.getName()), e);
          }
        }
      }
    }
  }

  private void txAutoFlushHandler() {
    log.debug("================ Queue transaction autoflush size reached, flushing offsets from connector");
    try {
      sinkTask.flush(offsets);
    } catch (ConnectException e) {
      if (e.getCause() instanceof JCSMPException) {
        throw new RetriableException(e.getMessage(), e.getCause());
      } else {
        throw e;
      }
    }
  }

  /**
   * Visible for testing.
   */
  void mayEnrichUserPropertiesWithKafkaRecordHeaders(final SinkRecord record,
                                                     final BytesXMLMessage message) {
    if (sconfig.isEmitKafkaRecordHeadersEnabled() && !record.headers().isEmpty()) {
      final SDTMap userMap = Optional
              .ofNullable(message.getProperties())
              .orElseGet(JCSMPFactory.onlyInstance()::createMap);
      record.headers().forEach(header -> {
        try {
          userMap.putObject(header.key(), header.value());
        } catch (SDTException e) {
          // Re-throw the exception because there is nothing else to do - usually that exception should not happen.
          throw new RuntimeException("Failed to add object message property from kafka record-header", e);
        }
      });
      message.setProperties(userMap);
    }
  }

  /**
   * Commit Solace and Kafka records.
   */
  public synchronized void commit() throws JCSMPException {
    if (producerHandler.getTxMsgCount().getAndSet(0) > 0) {
      sessionHandler.getTxSession().commit();
      log.debug("Committed Solace records for transaction with status: {}",
          sessionHandler.getTxSession().getStatus().name());
    }
  }

  /**
   * Shutdown TXProducer and Topic Producer.
   */
  public void shutdown() {
    producerHandler.close();
  }

}
