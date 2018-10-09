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
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.ProducerFlowProperties;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.SDTException;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageProducer;
import com.solacesystems.jcsmp.transaction.TransactedSession;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolaceSinkSender {
  private static final Logger log = LoggerFactory.getLogger(SolaceSinkSender.class);

  private SolaceSinkConfig sconfig;
  private XMLMessageProducer producer;
  private XMLMessageProducer txProducer;
  private JCSMPSession session;
  private BytesXMLMessage message;
  private List<Topic> topics = new ArrayList<Topic>();
  private Queue solQueue;
  private boolean useTxforQueue = false;
  private Class<?> cprocessor;
  private SolRecordProcessor processor;
  private String kafkaKey;
  private TransactedSession txSession;
  private SolaceSinkTask sinkTask;
  private AtomicInteger msgCounter = new AtomicInteger();
  private Map<TopicPartition, OffsetAndMetadata> offsets 
      = new HashMap<TopicPartition, OffsetAndMetadata>();

  /**
   * Class that sends Solace Messages from Kafka Records.
   * @param sconfig JCSMP Configuration
   * @param session JCSMPSession
   * @param txSession TransactedSession
   * @param sinkTask Connector Sink Task
   */
  public SolaceSinkSender(SolaceSinkConfig sconfig, JCSMPSession session, 
      TransactedSession txSession, SolaceSinkTask sinkTask) {
    this.sconfig = sconfig;
    this.txSession = txSession;
    this.sinkTask = sinkTask;
    this.session = session;

    kafkaKey = this.sconfig.getString(SolaceSinkConstants.SOL_KAFKA_MESSAGE_KEY);

    if (sconfig.getString(SolaceSinkConstants.SOl_QUEUE) != null) {
      solQueue = JCSMPFactory.onlyInstance().createQueue(
          sconfig.getString(SolaceSinkConstants.SOl_QUEUE));
    }

    ProducerFlowProperties flowProps = new ProducerFlowProperties();
    flowProps.setAckEventMode(sconfig.getString(SolaceSinkConstants.SOL_ACK_EVENT_MODE));
    flowProps.setWindowSize(sconfig.getInt(SolaceSinkConstants.SOL_PUBLISHER_WINDOW_SIZE));

    try {
      producer = session.getMessageProducer(new SolStreamingMessageCallbackHandler());
      if (sconfig.getString(SolaceSinkConstants.SOl_QUEUE) != null) {
        txProducer = txSession.createProducer(flowProps, new SolStreamingMessageCallbackHandler(),
            new SolProducerEventCallbackHandler());
        log.info("=================txSession status: {}", txSession.getStatus().toString());
      }

    } catch (JCSMPException e) {
      log.info("Received Solace exception {}, with the following: {} ", 
          e.getCause(), e.getStackTrace());
    }

    cprocessor = (this.sconfig.getClass(SolaceSinkConstants.SOL_RECORD_PROCESSOR));
    try {
      processor = (SolRecordProcessor) cprocessor.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      log.info("=================Received exception while creating record processing class {}, "
          + "with the following: {} ",
          e.getCause(), e.getStackTrace());
    }
  }

  /**
   * Generate Solace topics from topic string.
   */
  public void createTopics() {
    String solaceTopics = sconfig.getString(SolaceSinkConstants.SOL_TOPICS);
    String[] stopics = solaceTopics.split(",");
    int counter = 0;

    while (stopics.length > counter) {
      topics.add(JCSMPFactory.onlyInstance().createTopic(stopics[counter].trim()));
      counter++;
    }
  }

  public void useTx(boolean tx) {
    this.useTxforQueue = tx;
  }

  /**
   * Send Solace Message from Kafka Record.
   * @param record Kakfa Records
   */
  public void sendRecord(SinkRecord record) {
    message = processor.processRecord(kafkaKey, record);
    offsets.put(new TopicPartition(record.topic(), record.kafkaPartition()),
        new OffsetAndMetadata(record.kafkaOffset()));
    log.trace("=================record details, topic: {}, Partition: {}, "
        + "Offset: {}", record.topic(),
        record.kafkaPartition(), record.kafkaOffset());

    if (message.getAttachmentContentLength() == 0 || message.getAttachmentByteBuffer() == null) {
      log.info("==============Received record that had no data....discarded");
      return;
    }

    /*
    if (message.getUserData() == null) {
      log.trace("============Receive a Kafka record with no data ... discarded");
      return;
    }
    */
    
    // Use Dynamic destination from SolRecordProcessor
    if (sconfig.getBoolean(SolaceSinkConstants.SOL_DYNAMIC_DESTINATION)) {
      SDTMap userMap = message.getProperties();
      Destination dest = null;
      try {
        dest = userMap.getDestination("dynamicDestination");
      } catch (SDTException e) {
        log.info("=================Received exception retrieving Dynamic Destination:  "
            + "{}, with the following: {} ",
            e.getCause(), e.getStackTrace());
      }
      try {
        producer.send(message, dest);
      } catch (JCSMPException e) {
        log.trace(
            "=================Received exception while sending message to topic {}:  "
            + "{}, with the following: {} ",
            dest.getName(), e.getCause(), e.getStackTrace());
      }
      
      
    }
    

    if (useTxforQueue  && !(sconfig.getBoolean(SolaceSinkConstants.SOL_DYNAMIC_DESTINATION))) {
      try {
        message.setDeliveryMode(DeliveryMode.PERSISTENT);
        txProducer.send(message, solQueue);
        msgCounter.getAndIncrement();
        log.trace("===============Count of TX message is now: {}", msgCounter.get());
      } catch (JCSMPException e) {
        log.info("=================Received exception while sending message to queue {}:  "
            + "{}, with the following: {} ",
            solQueue.getName(), e.getCause(), e.getStackTrace());
      }

    }

    if (topics.size() != 0 && message.getDestination() == null 
          && !(sconfig.getBoolean(SolaceSinkConstants.SOL_DYNAMIC_DESTINATION))) {
      message.setDeliveryMode(DeliveryMode.DIRECT);
      int count = 0;
      while (topics.size() > count) {
        try {
          producer.send(message, topics.get(count));
          count++;
        } catch (JCSMPException e) {
          log.trace(
              "=================Received exception while sending message to topic {}:  "
              + "{}, with the following: {} ",
              topics.get(count).getName(), e.getCause(), e.getStackTrace());

        }
        count++;
      }

    }
    

    
    // Solace limits transaction size to 255 messages so need to force commit
    if (useTxforQueue && msgCounter.get() > 200) {
      log.debug("================Manually Flushing Offsets");
      sinkTask.flush(offsets);
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
        txSession.commit();
        commited = true;
        msgCounter.set(0);
        log.debug("Comitted Solace records for transaction with status: {}", 
            txSession.getStatus().name());
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
   * @return Boolean of Shutdown Status
   */
  public boolean shutdown() {
    if (txProducer != null) {
      txProducer.close();
    }
    
    if (producer != null) {
      producer.close();
    }
    if (session != null) {
      session.closeSession();
    }
    
    return true;
  }

}
