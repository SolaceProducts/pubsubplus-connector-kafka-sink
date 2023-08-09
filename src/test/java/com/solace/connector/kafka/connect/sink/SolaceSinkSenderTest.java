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
import com.solacesystems.jcsmp.transaction.RollbackException;
import com.solacesystems.jcsmp.transaction.TransactedSession;
import com.solacesystems.jcsmp.transaction.TransactionStatus;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class SolaceSinkSenderTest {

    @Mock private SolSessionHandler mkSessionHandler;
    @Mock private JCSMPSession mkJcsmpSession;
    @Mock private TransactedSession mkTransactedSession;
    @Mock private SolaceSinkTask mkSolaceSinkTask;

    @Test
    public void shouldAddKafkaRecordHeadersOnBytesXMLMessageWhenEnabled() throws JCSMPException {
        // GIVEN
        Mockito.when(mkSessionHandler.getSession()).thenReturn(mkJcsmpSession);
        Mockito.when(mkJcsmpSession.getMessageProducer(Mockito.any())).thenReturn(null);

        Map<String, String> config = new HashMap<>();
        config.put(SolaceSinkConstants.SOL_EMIT_KAFKA_RECORD_HEADERS_ENABLED, "true");
        config.put(SolaceSinkConstants.SOL_RECORD_PROCESSOR, SolSimpleRecordProcessor.class.getName());

        final SolaceSinkConnectorConfig connectorConfig = new SolaceSinkConnectorConfig(config);

        final SolaceSinkSender sender = new SolaceSinkSender(
                connectorConfig,
                mkSessionHandler,
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

    @ParameterizedTest(name = "[{index}] rollback={0}")
    @ValueSource(booleans = {true, false})
    public void testCommit(boolean rollback) throws Exception {
        Mockito.when(mkSessionHandler.getSession()).thenReturn(mkJcsmpSession);
        Mockito.when(mkSessionHandler.getTxSession()).thenReturn(mkTransactedSession);
        Mockito.when(mkTransactedSession.getStatus()).thenReturn(TransactionStatus.ACTIVE);

        if (rollback) {
            Mockito.doThrow(new RollbackException("test-rollback")).when(mkTransactedSession).commit();
        }

        Map<String, String> config = new HashMap<>();
        config.put(SolaceSinkConstants.SOL_RECORD_PROCESSOR, SolSimpleRecordProcessor.class.getName());
        config.put(SolaceSinkConstants.SOl_USE_TRANSACTIONS_FOR_QUEUE, Boolean.toString(true));
        config.put(SolaceSinkConstants.SOl_USE_TRANSACTIONS_FOR_TOPICS, Boolean.toString(true));

        final SolaceSinkConnectorConfig connectorConfig = new SolaceSinkConnectorConfig(config);
        final SolaceSinkSender sender = new SolaceSinkSender(connectorConfig, mkSessionHandler, mkSolaceSinkTask);

        sender.producerHandler.getTxMsgCount().incrementAndGet();
        if (rollback) {
            assertThrows(RollbackException.class, sender::commit);
        } else {
            sender.commit();
        }

        Mockito.verify(mkTransactedSession, Mockito.times(1)).commit();
        assertEquals(0, sender.producerHandler.getTxMsgCount().get());
    }

    @Test
    public void testCommitNoMessages() throws Exception {
        Mockito.when(mkSessionHandler.getSession()).thenReturn(mkJcsmpSession);
        Mockito.when(mkSessionHandler.getTxSession()).thenReturn(mkTransactedSession);

        Map<String, String> config = new HashMap<>();
        config.put(SolaceSinkConstants.SOL_RECORD_PROCESSOR, SolSimpleRecordProcessor.class.getName());
        config.put(SolaceSinkConstants.SOl_USE_TRANSACTIONS_FOR_QUEUE, Boolean.toString(true));
        config.put(SolaceSinkConstants.SOl_USE_TRANSACTIONS_FOR_TOPICS, Boolean.toString(true));

        final SolaceSinkConnectorConfig connectorConfig = new SolaceSinkConnectorConfig(config);
        final SolaceSinkSender sender = new SolaceSinkSender(connectorConfig, mkSessionHandler, mkSolaceSinkTask);
        sender.commit();

        Mockito.verify(mkTransactedSession, Mockito.times(0)).commit();
        assertEquals(0, sender.producerHandler.getTxMsgCount().get());
    }
}
