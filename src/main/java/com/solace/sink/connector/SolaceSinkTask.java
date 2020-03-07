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

import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPSessionStats;
import com.solacesystems.jcsmp.statistics.StatType;
import com.solacesystems.jcsmp.transaction.TransactedSession;

import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolaceSinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(SolaceSinkTask.class);
  private SolSessionCreate sessionRef;
  private TransactedSession txSession = null;
  private JCSMPSession session;
  private SolaceSinkSender sender;
  private boolean txEnabled = false;
  private SinkTaskContext context;

  SolaceSinkConfig sconfig;

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    sconfig = new SolaceSinkConfig(props);  

    sessionRef = new SolSessionCreate(sconfig);
    sessionRef.configureSession();
    sessionRef.connectSession();
    txSession = sessionRef.getTxSession();
    session = sessionRef.getSession();
    if (txSession != null) {
      log.info("======================TransactedSession JCSMPSession Connected");
    }

    sender = new SolaceSinkSender(sconfig, session, txSession, this);

    if (sconfig.getString(SolaceSinkConstants.SOL_TOPICS) != null) {
      sender.createTopics();
    }
    if (sconfig.getString(SolaceSinkConstants.SOl_QUEUE) != null) {
      
        // TODO: Fix logic, as this is assumed to be TRUE even if code changed here
        txEnabled = true;
      sender.useTx(txEnabled);
    }

  }

  @Override
  public void put(Collection<SinkRecord> records) {
    for (SinkRecord r : records) {
      log.trace("Putting record to topic {}, partition {} and offset {}", r.topic(), 
          r.kafkaPartition(),
          r.kafkaOffset());
      sender.sendRecord(r);
    }

  }

  @Override
  public void stop() {
    if (session != null) {
      JCSMPSessionStats lastStats = session.getSessionStats();
      Enumeration<StatType> estats = StatType.elements();
      log.info("Final Statistics summary:");

      while (estats.hasMoreElements()) {
        StatType statName = estats.nextElement();
        System.out.println("\t" + statName.getLabel() + ": " + lastStats.getStat(statName));
      }
      log.info("\n");
    }
    boolean ok = true;
    log.info("==================Shutting down Solace Source Connector");

    if (sender != null) {
      ok = sender.shutdown();
    }
    if (session != null) {
      ok = sessionRef.shutdown();
    }

    if (!(ok)) {
      log.info("Solace session failed to shutdown");
    }

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

    // TODO: how the transacted flag is taken into consideration?
    if (sconfig.getString(SolaceSinkConstants.SOl_QUEUE) != null) {
      boolean commited = sender.commit();
      if (!commited) {
        log.info("==============error in commiting transaction, shutting down");
        stop();
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
    Long offsetLong = sconfig.getLong(SolaceSinkConstants.SOL_KAFKA_REPLAY_OFFSET);
    log.debug("================Starting  for replay Offset: " + offsetLong);
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
