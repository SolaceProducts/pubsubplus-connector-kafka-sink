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
    Schema s = record.valueSchema();

    String kafkaTopic = record.topic();

    // Add Record Topic,Parition,Offset to Solace Msg in case we need to track offset restart
    String userData = "T:" + record.topic() + ",P:" + record.kafkaPartition() 
        + ",O:" + record.kafkaOffset();
    msg.setUserData(userData.getBytes(StandardCharsets.UTF_8)); 
    msg.setApplicationMessageType("ResendOfKakfaTopic: " + kafkaTopic);
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
    } else if (s.type() == Schema.Type.BYTES) {
      if (v instanceof byte[]) {
        msg.writeAttachment((byte[]) v);
      } else if (v instanceof ByteBuffer) {
        msg.writeAttachment((byte[]) ((ByteBuffer) v).array());
      }
    }
    return msg;
  }

}
