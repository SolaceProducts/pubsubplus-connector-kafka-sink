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

package com.solace.sink.connector;

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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolaceSinkSender {
  private static final Logger log = LoggerFactory.getLogger(SolaceSinkSender.class);

  private SolaceSinkConnectorConfig sconfig;
  private XMLMessageProducer topicProducer;
  private XMLMessageProducer queueProducer;
  private SolSessionHandler sessionHandler;
  private BytesXMLMessage message;
  private List<Topic> topics = new ArrayList<Topic>();
  private Queue solQueue = null;
  private boolean useTxforQueue = false;
  private Class<?> cprocessor;
  private SolRecordProcessorIF processor;
  private String kafkaKey;
  private AtomicInteger txMsgCounter = new AtomicInteger();

  /**
   * Class that sends Solace Messages from Kafka Records.
   * @param sconfig JCSMP Configuration
   * @param sessionHandler SolSessionHandler
   * @param useTxforQueue 
   * @throws JCSMPException 
   */
  public SolaceSinkSender(SolaceSinkConnectorConfig sconfig, SolSessionHandler sessionHandler, 
      boolean useTxforQueue) throws JCSMPException {
    this.sconfig = sconfig;
    this.sessionHandler = sessionHandler;
    this.useTxforQueue = useTxforQueue;
    kafkaKey = sconfig.getString(SolaceSinkConstants.SOL_KAFKA_MESSAGE_KEY);
    topicProducer = sessionHandler.getSession().getMessageProducer(new SolStreamingMessageCallbackHandler());
    cprocessor = (this.sconfig.getClass(SolaceSinkConstants.SOL_RECORD_PROCESSOR));
    try {
      processor = (SolRecordProcessorIF) cprocessor.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      log.info("================ Received exception while creating record processing class {}, "
          + "with the following: {} ",
          e.getCause(), e.getStackTrace());
    }
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
    message = processor.processRecord(kafkaKey, record);
    log.trace("================ Processed record details, topic: {}, Partition: {}, "
        + "Offset: {}", record.topic(),
        record.kafkaPartition(), record.kafkaOffset());

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
        log.info("================ Received exception retrieving Dynamic Destination:  "
            + "{}, with the following: {} ",
            e.getCause(), e.getStackTrace());
      }
      try {
        topicProducer.send(message, dest);
      } catch (JCSMPException e) {
        log.info(
            "================ Received exception while sending message to topic {}:  "
            + "{}, with the following: {} ",
            dest.getName(), e.getCause(), e.getStackTrace());
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
        } catch (JCSMPException e) {
          log.info("================ Received exception while sending message to queue {}:  "
              + "{}, with the following: {} ",
              solQueue.getName(), e.getCause(), e.getStackTrace());
        }
      }
      if (topics.size() != 0 && message.getDestination() == null) {
        message.setDeliveryMode(DeliveryMode.DIRECT);
        int count = 0;
        while (topics.size() > count) {
          try {
            topicProducer.send(message, topics.get(count));
          } catch (JCSMPException e) {
            log.trace(
                "================ Received exception while sending message to topic {}:  "
                + "{}, with the following: {} ",
                topics.get(count).getName(), e.getCause(), e.getStackTrace());
          }
          count++;
        }
      }
    }
    
    // Solace limits transaction size to 255 messages so need to force commit
    if ( useTxforQueue && txMsgCounter.get() > sconfig.getInt(SolaceSinkConstants.SOL_QUEUE_MESSAGES_AUTOFLUSH_SIZE)-1 ) {
      log.debug("================ Queue transaction autoflush size reached, flushing offsets from connector");
      commit();
    }
  }
  
  /**
   * Commit Solace and Kafka records.
   * @return Boolean Status
   */
  public synchronized boolean commit() {
    boolean commited = true;
    try {
      if (useTxforQueue) {
        sessionHandler.getTxSession().commit();
        commited = true;
        txMsgCounter.set(0);
        log.debug("Comitted Solace records for transaction with status: {}", 
            sessionHandler.getTxSession().getStatus().name());
      }
    } catch (JCSMPException e) {
      log.info("Received Solace TX exception {}, with the following: {} ", 
          e.getCause(), e.getStackTrace());
      log.info("The TX error could be due to using dynamic destinations and "
          + "  \"sol.dynamic_destination=true\" was not set in the configuration ");
      commited = false;
    }
    return commited;
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
