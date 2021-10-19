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

package com.solace.connector.kafka.connect.sink.recordprocessor;

import com.solace.connector.kafka.connect.sink.SolRecordProcessorIF;
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

public class SolSimpleRecordProcessor implements SolRecordProcessorIF {
  private static final Logger log = LoggerFactory.getLogger(SolSimpleRecordProcessor.class);

  @Override
  public BytesXMLMessage processRecord(String skey, SinkRecord record) {
    BytesXMLMessage msg = JCSMPFactory.onlyInstance().createMessage(BytesXMLMessage.class);

    // Add Record Topic,Partition,Offset to Solace Msg
    String kafkaTopic = record.topic();
    SDTMap userHeader = JCSMPFactory.onlyInstance().createMap();
    try {
      userHeader.putString("k_topic", kafkaTopic);
      userHeader.putInteger("k_partition", record.kafkaPartition());
      userHeader.putLong("k_offset", record.kafkaOffset());
    } catch (SDTException e) {
      log.info("Received Solace SDTException {}, with the following: {} ",
          e.getCause(), e.getStackTrace());
    }
    msg.setProperties(userHeader);
    msg.setApplicationMessageType("ResendOfKafkaTopic: " + kafkaTopic);

    Schema valueSchema = record.valueSchema();
    Object recordValue = record.value();
    // get message body details from record
    if (recordValue != null) {
      if (valueSchema == null) {
        log.trace("No schema info {}", recordValue);
        if (recordValue instanceof byte[]) {
          msg.writeAttachment((byte[]) recordValue);
        } else if (recordValue instanceof ByteBuffer) {
          msg.writeAttachment((byte[]) ((ByteBuffer) recordValue).array());
        } else if (recordValue instanceof String) {
          msg.writeAttachment(((String) recordValue).getBytes(StandardCharsets.UTF_8));
        } else {
          // Unknown recordValue type
          msg.reset();
        }
      } else if (valueSchema.type() == Schema.Type.BYTES) {
        if (recordValue instanceof byte[]) {
          msg.writeAttachment((byte[]) recordValue);
        } else if (recordValue instanceof ByteBuffer) {
          msg.writeAttachment((byte[]) ((ByteBuffer) recordValue).array());
        }
      } else if (valueSchema.type() == Schema.Type.STRING) {
        msg.writeAttachment(((String) recordValue).getBytes(StandardCharsets.UTF_8));
      } else {
        // Do nothing in all other cases
        msg.reset();
      }
    } else {
      // Invalid message
      msg.reset();
    }

    return msg;
  }

}
