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

import com.solacesystems.jcsmp.*;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Map;

public class SolaceSinkSenderTest {

    private SolSessionHandler mkSessionHandler;
    private JCSMPSession mkJcsmpSession;
    private SolaceSinkTask mkSolaceSinkTask;

    @Before
    public void setUp() {
        mkSessionHandler = Mockito.mock(SolSessionHandler.class);
        mkSolaceSinkTask = Mockito.mock(SolaceSinkTask.class);
        mkJcsmpSession = Mockito.mock(JCSMPSession.class);
    }

    @Test
    public void shouldAddKafkaRecordHeadersOnBytesXMLMessageWhenEnabled() throws JCSMPException {
        // GIVEN
        Mockito.when(mkSessionHandler.getSession()).thenReturn(mkJcsmpSession);
        Mockito.when(mkJcsmpSession.getMessageProducer(Mockito.any())).thenReturn(null);

        final SolaceSinkConnectorConfig connectorConfig = new SolaceSinkConnectorConfig(
                Map.of(SolaceSinkConstants.SOL_EMIT_KAFKA_RECORD_HEADERS_ENABLED, "true")
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
        Assert.assertNotNull(properties);
        Assert.assertEquals("val1", properties.getString("h1"));
        Assert.assertEquals("val2", properties.getString("h2"));
        Assert.assertEquals("val5", properties.getString("h3"));
    }
}