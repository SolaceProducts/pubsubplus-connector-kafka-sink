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

public class SolSimpleKeyedRecordProcessor implements SolRecordProcessorIF {
  private static final Logger log = LoggerFactory.getLogger(SolSimpleKeyedRecordProcessor.class);

  public enum KeyHeader {
    NONE, DESTINATION, CORRELATION_ID, CORRELATION_ID_AS_BYTES
  }

  protected KeyHeader keyheader = KeyHeader.NONE; // default

  @Override
  public BytesXMLMessage processRecord(String skey, SinkRecord record) {
    if (skey.equals("NONE")) {
      this.keyheader = KeyHeader.NONE;
    } else if (skey.equals("DESTINATION")) {
      this.keyheader = KeyHeader.DESTINATION;
    } else if (skey.equals("CORRELATION_ID")) {
      this.keyheader = KeyHeader.CORRELATION_ID;
    } else if (skey.equals("CORRELATION_ID_AS_BYTES")) {
      this.keyheader = KeyHeader.CORRELATION_ID_AS_BYTES;
    }

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

    Object recordKey = record.key();
    Schema keySchema = record.keySchema();

    // If Topic was Keyed, use the key for correlationID
    if (keyheader == KeyHeader.CORRELATION_ID || keyheader == KeyHeader.CORRELATION_ID_AS_BYTES) {
      if (recordKey != null) {
        if (keySchema == null) {
          log.trace("No schema info {}", recordKey);
          if (recordKey instanceof byte[]) {
            msg.setCorrelationId(new String((byte[]) recordKey, StandardCharsets.UTF_8));
          } else if (recordKey instanceof ByteBuffer) {
            msg.setCorrelationId(new String(((ByteBuffer) recordKey).array(), StandardCharsets.UTF_8));
          } else {
            msg.setCorrelationId(recordKey.toString());
          }
        } else if (keySchema.type() == Schema.Type.BYTES) {
          if (recordKey instanceof byte[]) {
            msg.setCorrelationId(new String((byte[]) recordKey, StandardCharsets.UTF_8));
          } else if (recordKey instanceof ByteBuffer) {
            msg.setCorrelationId(new String(((ByteBuffer) recordKey).array(), StandardCharsets.UTF_8));
          }
        } else if (keySchema.type() == Schema.Type.STRING) {
          msg.setCorrelationId((String) recordKey);
        } else {
          log.trace("No applicable schema type {}", keySchema.type());
          // Nothing to do with no applicable schema type
        }
      } else {
        // Nothing to do with null recordKey
      }
    } else if (keyheader == KeyHeader.DESTINATION && keySchema.type() == Schema.Type.STRING) {
        // Destination is already determined by sink settings so set just the correlationId.
        // Receiving app can evaluate it
        msg.setCorrelationId((String) recordKey);
    } else {
      // Do nothing in all other cases
    }

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
