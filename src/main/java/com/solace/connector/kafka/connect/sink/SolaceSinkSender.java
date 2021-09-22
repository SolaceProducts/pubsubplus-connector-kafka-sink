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
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.ProducerFlowProperties;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.SDTException;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageProducer;
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
import java.util.concurrent.atomic.AtomicInteger;

public class SolaceSinkSender {
  private static final Logger log = LoggerFactory.getLogger(SolaceSinkSender.class);

  private SolaceSinkConnectorConfig sconfig;
  private XMLMessageProducer topicProducer;
  private XMLMessageProducer queueProducer;
  private SolSessionHandler sessionHandler;
  private List<Topic> topics = new ArrayList<Topic>();
  private Queue solQueue = null;
  private boolean useTxforQueue = false;
  private SolRecordProcessorIF processor;
  private String kafkaKey;
  private AtomicInteger txMsgCounter = new AtomicInteger();
  private SolaceSinkTask sinkTask;
  private Map<TopicPartition, OffsetAndMetadata> offsets
      = new HashMap<TopicPartition, OffsetAndMetadata>();

  /**
   * Class that sends Solace Messages from Kafka Records.
   * @param sconfig JCSMP Configuration
   * @param sessionHandler SolSessionHandler
   * @param useTxforQueue
   * @param sinkTask Connector Sink Task
   * @throws JCSMPException
   */
  public SolaceSinkSender(SolaceSinkConnectorConfig sconfig, SolSessionHandler sessionHandler,
      boolean useTxforQueue, SolaceSinkTask sinkTask) throws JCSMPException {
    this.sconfig = sconfig;
    this.sessionHandler = sessionHandler;
    this.useTxforQueue = useTxforQueue;
    this.sinkTask = sinkTask;
    kafkaKey = sconfig.getString(SolaceSinkConstants.SOL_KAFKA_MESSAGE_KEY);
    topicProducer = sessionHandler.getSession().getMessageProducer(new SolStreamingMessageCallbackHandler());
    processor = this.sconfig.getConfiguredInstance(SolaceSinkConstants.SOL_RECORD_PROCESSOR, SolRecordProcessorIF.class);
  }

  /**
   * Generate PubSub+ topics from topic string
   */
  public void setupDestinationTopics() {
    String solaceTopics = sconfig.getString(SolaceSinkConstants.SOL_TOPICS);
    String[] stopics = solaceTopics.split(",");
    int counter = 0;
    while (stopics.length > counter) {
      topics.add(JCSMPFactory.onlyInstance().createTopic(stopics[counter].trim()));
      counter++;
    }
  }

  /**
   * Generate PubSub queue
   */
  public void setupDestinationQueue() throws JCSMPException {
    solQueue = JCSMPFactory.onlyInstance().createQueue(sconfig.getString(SolaceSinkConstants.SOl_QUEUE));
    ProducerFlowProperties flowProps = new ProducerFlowProperties();
    flowProps.setAckEventMode(sconfig.getString(SolaceSinkConstants.SOL_ACK_EVENT_MODE));
    flowProps.setWindowSize(sconfig.getInt(SolaceSinkConstants.SOL_PUBLISHER_WINDOW_SIZE));
    if (useTxforQueue) {
      // Using transacted session for queue
      queueProducer = sessionHandler.getTxSession().createProducer(flowProps, new SolStreamingMessageCallbackHandler(),
          new SolProducerEventCallbackHandler());
      log.info("================ txSession status: {}", sessionHandler.getTxSession().getStatus().toString());
    } else {
      // Not using transacted session for queue
      queueProducer = sessionHandler.getSession().createProducer(flowProps, new SolStreamingMessageCallbackHandler(),
          new SolProducerEventCallbackHandler());
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
      log.trace("================ Processed record details, topic: {}, Partition: {}, "
          + "Offset: {}", record.topic(),
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
        topicProducer.send(message, dest);
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
          message.setDeliveryMode(DeliveryMode.PERSISTENT);
          queueProducer.send(message, solQueue);
          if (useTxforQueue) {
            txMsgCounter.getAndIncrement();
            log.trace("================ Count of TX message is now: {}", txMsgCounter.get());
          }
        } catch (IllegalArgumentException e) {
          throw new ConnectException(String.format("Received exception while sending message to queue %s",
                  solQueue.getName()), e);
        } catch (JCSMPException e) {
          throw new RetriableException(String.format("Received exception while sending message to queue %s",
                  solQueue.getName()), e);
        }
      }
      if (topics.size() != 0 && message.getDestination() == null) {
        message.setDeliveryMode(DeliveryMode.DIRECT);
        int count = 0;
        while (topics.size() > count) {
          try {
            topicProducer.send(message, topics.get(count));
          } catch (IllegalArgumentException e) {
            throw new ConnectException(String.format("Received exception while sending message to topic %s",
                    topics.get(count).getName()), e);
          } catch (JCSMPException e) {
            throw new RetriableException(String.format("Received exception while sending message to topic %s",
                    topics.get(count).getName()), e);
          }
          count++;
        }
      }
    }

    // Solace limits transaction size to 255 messages so need to force commit
    if ( useTxforQueue && txMsgCounter.get() > sconfig.getInt(SolaceSinkConstants.SOL_QUEUE_MESSAGES_AUTOFLUSH_SIZE)-1 ) {
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
  }

  /**
   * Commit Solace and Kafka records.
   */
  public synchronized void commit() throws JCSMPException {
    if (useTxforQueue) {
      sessionHandler.getTxSession().commit();
      txMsgCounter.set(0);
      log.debug("Comitted Solace records for transaction with status: {}",
          sessionHandler.getTxSession().getStatus().name());
    }
  }

  /**
   * Shutdown TXProducer and Topic Producer.
   * @return
   */
  public void shutdown() {
    if (queueProducer != null) {
      queueProducer.close();
    }
    if (topicProducer != null) {
      topicProducer.close();
    }
  }

}
