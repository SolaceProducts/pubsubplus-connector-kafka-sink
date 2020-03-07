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

package com.solace.sink.connector.recordprocessor;

import com.solace.sink.connector.SolRecordProcessor;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.SDTException;
import com.solacesystems.jcsmp.SDTMap;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolSimpleRecordProcessor implements SolRecordProcessor {
  private static final Logger log = LoggerFactory.getLogger(SolSimpleRecordProcessor.class);

  @Override
  public BytesXMLMessage processRecord(String skey, SinkRecord record) {
    BytesXMLMessage msg = JCSMPFactory.onlyInstance().createMessage(BytesXMLMessage.class);
    

    // Add Record Topic,Partition,Offset to Solace Msg in case we need to track offset restart
    // limited in Kafka Topic size, replace using SDT below.
    //String userData = "T:" + record.topic() + ",P:" + record.kafkaPartition() 
    //    + ",O:" + record.kafkaOffset();
    //msg.setUserData(userData.getBytes(StandardCharsets.UTF_8)); 
    
    // Add Record Topic,Partition,Offset to Solace Msg as header properties 
    // in case we need to track offset restart
    SDTMap userHeader = JCSMPFactory.onlyInstance().createMap();
    try {
      userHeader.putString("k_topic", record.topic());
      userHeader.putInteger("k_partition", record.kafkaPartition());
      userHeader.putLong("k_offset", record.kafkaOffset());
    } catch (SDTException e) {
      log.info("Received Solace SDTException {}, with the following: {} ", 
          e.getCause(), e.getStackTrace());
    }
    msg.setProperties(userHeader);
    
    Schema s = record.valueSchema();
    String kafkaTopic = record.topic();
    
    msg.setApplicationMessageType("ResendOfKafkaTopic: " + kafkaTopic);
    Object v = record.value();
    log.debug("Value schema {}", s);
    if (v == null) {
      msg.reset();
      return msg;
    } else if (s == null) {
      log.debug("No schema info {}", v);
      if (v instanceof byte[]) {
        msg.writeAttachment((byte[]) v);
      } else if (v instanceof ByteBuffer) {
        msg.writeAttachment((byte[]) ((ByteBuffer) v).array());
      }
      // TODO: how about String?
      // TODO: log nothing found
    } else if (s.type() == Schema.Type.BYTES) {
      if (v instanceof byte[]) {
        msg.writeAttachment((byte[]) v);
      } else if (v instanceof ByteBuffer) {
        msg.writeAttachment((byte[]) ((ByteBuffer) v).array());
      }
      // TODO: log nothing found
    }
    // TODO: log if unknown schema = message loss
    
    
    return msg;
  }

}
