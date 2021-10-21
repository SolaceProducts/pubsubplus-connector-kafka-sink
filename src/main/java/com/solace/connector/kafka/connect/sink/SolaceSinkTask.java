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

import com.solacesystems.jcsmp.JCSMPException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class SolaceSinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(SolaceSinkTask.class);
  private SolSessionHandler solSessionHandler;
  private SolaceSinkSender solSender;
  private boolean useTxforQueue = false;
  private SinkTaskContext context;

  SolaceSinkConnectorConfig connectorConfig;

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    connectorConfig = new SolaceSinkConnectorConfig(props);
    solSessionHandler = new SolSessionHandler(connectorConfig);
    try {
      solSessionHandler.configureSession();
      solSessionHandler.connectSession();
    } catch (JCSMPException e) {
      throw new ConnectException("Failed to create JCSMPSession", e);
    }
    log.info("================ JCSMPSession Connected");

    if (connectorConfig.getString(SolaceSinkConstants.SOl_QUEUE) != null) {
      // Use transactions for queue destination
      useTxforQueue = connectorConfig.getBoolean(SolaceSinkConstants.SOl_USE_TRANSACTIONS_FOR_QUEUE);
      if (useTxforQueue) {
        try {
          solSessionHandler.createTxSession();
          log.info("================ Transacted Session has been Created for PubSub+ queue destination");
        } catch (JCSMPException e) {
          throw new ConnectException("Failed to create Transacted Session for PubSub+ queue destination, " +
                  "make sure transacted sessions are enabled", e);
        }
      }
    }

    try {
      solSender = new SolaceSinkSender(connectorConfig, solSessionHandler, useTxforQueue, this);
      if (connectorConfig.getString(SolaceSinkConstants.SOL_TOPICS) != null) {
        solSender.setupDestinationTopics();
      }
      if (connectorConfig.getString(SolaceSinkConstants.SOl_QUEUE) != null) {
        solSender.setupDestinationQueue();
      }
    } catch (Exception e) {
      throw new ConnectException("Failed to setup sender to PubSub+", e);
    }
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    for (SinkRecord r : records) {
      log.trace("Putting record to topic {}, partition {} and offset {}", r.topic(),
          r.kafkaPartition(),
          r.kafkaOffset());
      solSender.sendRecord(r);
    }
  }

  @Override
  public void stop() {
    log.info("================ Shutting down PubSub+ Sink Connector");
    if (solSender != null) {
      solSender.shutdown();
    }
    if (solSessionHandler != null) {
      log.info("Final Statistics summary:\n");
      solSessionHandler.printStats();
      solSessionHandler.shutdown();
    }
    log.info("PubSub+ Sink Connector stopped");
  }

  /**
   * Flushes Kafka Records.
   */
  public synchronized void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : currentOffsets.entrySet()) {
      TopicPartition tp = entry.getKey();
      OffsetAndMetadata om = entry.getValue();
      log.debug("Flushing up to topic {}, partition {} and offset {}", tp.topic(),
          tp.partition(), om.offset());
    }
    if (useTxforQueue) {
      try {
        solSender.commit();
      } catch (JCSMPException e) {
        // Consider using RetriableException if the Kafka Connect API one day decides to support it for flush/commit
        throw new ConnectException("Error in committing transaction. The TX error could be due to using dynamic " +
                "destinations and \"sol.dynamic_destination=true\" was not set in the configuration.", e);
      }
    }
  }

  /**
   * Create reference for SinkTaskContext - required for replay.
   *
   * @param context SinkTaskContext
   */
  public void initialize(SinkTaskContext context) {
    this.context = context;
  }

  /**
   * Opens access to partition write, this populates SinkTask Context
   * which allows setting of offset from which to start reading.
   *
   * @param partitions List of TopicPartitions for Topic
   */
  public void open(Collection<TopicPartition> partitions) {
    Long offsetLong = connectorConfig.getLong(SolaceSinkConstants.SOL_KAFKA_REPLAY_OFFSET);
    log.debug("================ Starting  for replay Offset: " + offsetLong);
    if (offsetLong != null) {
      Set<TopicPartition> parts = context.assignment();
      Iterator<TopicPartition> partsIt = parts.iterator();
      while (partsIt.hasNext()) {
        TopicPartition tp = partsIt.next();
        context.offset(tp, offsetLong);
      }
    }
  }

}
