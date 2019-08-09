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
import com.solacesystems.jcsmp.Topic;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.kafka.connect.sink.SinkRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Note: this example expects a record written to a Kafka topic that has the format:
 * "busId" "Message", where there is a space in between the strings. 
 * 
 * It requires the configuration property "sol.dynamic_destination=true" to be set. 
 */

public class SolDynamicDestinationRecordProcessor implements SolRecordProcessor {
  private static final Logger log = 
      LoggerFactory.getLogger(SolDynamicDestinationRecordProcessor.class);

  @Override
  public BytesXMLMessage processRecord(String skey, SinkRecord record) {
    BytesXMLMessage msg = JCSMPFactory.onlyInstance().createMessage(BytesXMLMessage.class);
    
    // Add Record Topic,Parition,Offset to Solace Msg in case we need to track offset restart
    // limited in Kafka Topic size, replace using SDT below.
    //String userData = "T:" + record.topic() + ",P:" + record.kafkaPartition() 
    //    + ",O:" + record.kafkaOffset();
    //msg.setUserData(userData.getBytes(StandardCharsets.UTF_8)); 


    
    Object v = record.value();
    String payload = "";
    Topic topic;
    if (v instanceof byte[]) {
      payload = new String((byte[]) v, StandardCharsets.UTF_8);
    } else if (v instanceof ByteBuffer) {
      payload = new String(((ByteBuffer) v).array(),StandardCharsets.UTF_8);
    }
    
    log.debug("==============================Payload: " + payload);
    
    String busId = payload.substring(0, 4);
    
    String busMsg = payload.substring(5, payload.length());
    log.debug("=================bus message: " + busMsg);
    
    if (busMsg.toLowerCase().contains("stop")) {
      msg.writeAttachment(busMsg.getBytes(StandardCharsets.UTF_8));
      topic = JCSMPFactory.onlyInstance().createTopic("ctrl/bus/" + busId + "/stop");
      log.debug("=========================Dynamic Topic = " + topic.getName());

    } else if (busMsg.toLowerCase().contains("start")) {
      msg.writeAttachment(busMsg.getBytes(StandardCharsets.UTF_8));
      topic = JCSMPFactory.onlyInstance().createTopic("ctrl/bus/" + busId + "/start");
      log.debug("=========================Dynamic Topic = " + topic.getName());
    } else {
      topic = JCSMPFactory.onlyInstance().createTopic("comms/bus/" + busId);   
      log.debug("=========================Dynamic Topic = " + topic.getName());
    }
    
    
    
    
    // Add Record Topic,Partition,Offset to Solace Msg as header properties 
    // in case we need to track offset restart
    SDTMap userHeader = JCSMPFactory.onlyInstance().createMap();
    try {
      userHeader.putString("k_topic", record.topic());
      userHeader.putInteger("k_partition", record.kafkaPartition());
      userHeader.putLong("k_offset", record.kafkaOffset());
      userHeader.putDestination("dynamicDestination", topic);
    } catch (SDTException e) {
      log.info("Received Solace SDTException {}, with the following: {} ", 
          e.getCause(), e.getStackTrace());
    }
    
    String kafkaTopic = record.topic();
    
    msg.setApplicationMessageType("ResendOfKakfaTopic: " + kafkaTopic);
    
    msg.setProperties(userHeader);

    log.debug("=================bus message: " + busMsg);
    
    msg.writeAttachment(busMsg.getBytes(StandardCharsets.UTF_8));
    
    
    return msg;
  }

}
