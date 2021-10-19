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

import org.apache.kafka.common.Configurable;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Map;

public interface SolRecordProcessorIF extends Configurable {

  /**
   * {@inheritDoc}
   */
  @Override
  default void configure(Map<String, ?> configs) { }

  /**
   * Converts a record consumed from Kafka into a Solace {@link BytesXMLMessage}.
   *
   * @param skey    the Kafka record-key.
   * @param record  the Kafka record-value.
   * @return        a new {@link BytesXMLMessage}.
   */
  BytesXMLMessage processRecord(String skey, SinkRecord record);

}