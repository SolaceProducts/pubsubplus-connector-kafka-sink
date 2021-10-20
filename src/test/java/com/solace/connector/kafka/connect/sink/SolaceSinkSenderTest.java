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

import com.solace.connector.kafka.connect.sink.recordprocessor.SolSimpleRecordProcessor;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.SDTMap;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(MockitoExtension.class)
public class SolaceSinkSenderTest {

    @Mock private SolSessionHandler mkSessionHandler;
    @Mock private JCSMPSession mkJcsmpSession;
    @Mock private SolaceSinkTask mkSolaceSinkTask;

    @Test
    public void shouldAddKafkaRecordHeadersOnBytesXMLMessageWhenEnabled() throws JCSMPException {
        // GIVEN
        Mockito.when(mkSessionHandler.getSession()).thenReturn(mkJcsmpSession);
        Mockito.when(mkJcsmpSession.getMessageProducer(Mockito.any())).thenReturn(null);

        final SolaceSinkConnectorConfig connectorConfig = new SolaceSinkConnectorConfig(
                Map.of(SolaceSinkConstants.SOL_EMIT_KAFKA_RECORD_HEADERS_ENABLED, "true",
                        SolaceSinkConstants.SOL_RECORD_PROCESSOR, SolSimpleRecordProcessor.class.getName())
        );

        final SolaceSinkSender sender = new SolaceSinkSender(
                connectorConfig,
                mkSessionHandler,
                false,
                mkSolaceSinkTask
        );

        ConnectHeaders headers = new ConnectHeaders();

        headers.addString("h2", "val2");
        headers.addString("h3", "val3");
        headers.addString("h3", "val4");
        headers.addString("h3", "val5");

        SinkRecord record = new SinkRecord(
                "topic",
                0,
                Schema.STRING_SCHEMA,
                "key",
                Schema.STRING_SCHEMA,
                "value",
                0L,
                0L,
                TimestampType.CREATE_TIME,
                headers
        );

        // WHEN
        BytesXMLMessage msg = JCSMPFactory.onlyInstance().createMessage(BytesXMLMessage.class);

        SDTMap existing = JCSMPFactory.onlyInstance().createMap();
        existing.putString("h1", "val1");
        msg.setProperties(existing);
        sender.mayEnrichUserPropertiesWithKafkaRecordHeaders(record, msg);

        // THEN
        SDTMap properties = msg.getProperties();
        assertNotNull(properties);
        assertEquals("val1", properties.getString("h1"));
        assertEquals("val2", properties.getString("h2"));
        assertEquals("val5", properties.getString("h3"));
    }
}