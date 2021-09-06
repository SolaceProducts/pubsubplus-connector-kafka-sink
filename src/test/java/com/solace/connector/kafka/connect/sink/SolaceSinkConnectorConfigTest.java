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
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class SolaceSinkConnectorConfigTest {

    @Test
    public void shouldReturnConfiguredSolRecordProcessorIFGivenConfigurableClass() {
        // GIVEN
        Map<String, String> configProps = new HashMap<>() {{
            put("processor.config", "dummy");
            put(SolaceSinkConstants.SOL_RECORD_PROCESSOR, TestSolRecordProcessorIF.class.getName());
        }};

        // WHEN
        SolaceSinkConnectorConfig config = new SolaceSinkConnectorConfig(configProps);

        // THEN
        SolRecordProcessorIF processor = config.getSolRecordProcessor();
        Assert.assertNotNull(processor);
        Assert.assertNotNull(((TestSolRecordProcessorIF)processor).configs);
        Assert.assertEquals("dummy", ((TestSolRecordProcessorIF)processor).configs.get("processor.config"));

    }

    public static class TestSolRecordProcessorIF implements SolRecordProcessorIF {

        Map<String, ?> configs;

        @Override
        public void configure(Map<String, ?> configs) {
           this.configs = configs;
        }

        @Override
        public BytesXMLMessage processRecord(String skey, SinkRecord record) {
            return null;
        }
    }
}