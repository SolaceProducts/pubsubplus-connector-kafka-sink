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

package com.solace.sink.connector;


import java.util.Collection;
import java.util.Enumeration;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPSessionStats;
import com.solacesystems.jcsmp.statistics.StatType;
import com.solacesystems.jcsmp.transaction.TransactedSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolaceSinkTask extends SinkTask {
	private static final Logger log = LoggerFactory.getLogger(SolaceSinkTask.class);
	private SolSessionCreate sessionRef;
	private TransactedSession TxSession = null;
	private JCSMPSession session;
	private SolaceSinkSender sender;
	private boolean txEnabled = false;

	SolaceSinkConfig sConfig;

	@Override
	public String version() {
		return VersionUtil.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		sConfig = new SolaceSinkConfig(props);

		sessionRef = new SolSessionCreate(sConfig);
		sessionRef.configureSession();
		sessionRef.connectSession();
		TxSession = sessionRef.getTxSession();
		session = sessionRef.getSession();
		if(TxSession != null) 
			log.info("======================TransactedSession JCSMPSession Connected");
		
		sender = new SolaceSinkSender(sConfig, session, TxSession, this);

		if (sConfig.getString(SolaceSinkConstants.SOL_TOPICS) != null) {
			sender.createTopics();
		}
		if (sConfig.getString(SolaceSinkConstants.SOl_QUEUE) != null) {
			txEnabled = true;
			sender.useTx(txEnabled);
		}

	}

	@Override
	public void put(Collection<SinkRecord> records) {
		for (SinkRecord r: records) {
			log.trace("Putting record to topic {}, partition {} and offset {}", r.topic(), r.kafkaPartition(), r.kafkaOffset());
			sender.sendRecord(r);
		}

	}

	@Override
	public void stop() {
		if(session != null) {
			JCSMPSessionStats lastStats = session.getSessionStats();
			Enumeration<StatType> eStats = StatType.elements();
			log.info("Final Statistics summary:");

			while (eStats.hasMoreElements()) {
				StatType statName = eStats.nextElement();
				System.out.println("\t" + statName.getLabel() + ": " + lastStats.getStat(statName));
			}
			log.info("\n");
		}
		boolean OK = true;
		log.info("==================Shutting down Solace Source Connector");

		if(sender != null)
			OK = sender.shutdown();
		if(session != null)
			OK = sessionRef.shutdown();

		if(!(OK)) 
			log.info("Solace session failed to shutdown");

	}

	public synchronized void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
		for (Map.Entry<TopicPartition, OffsetAndMetadata> entry: currentOffsets.entrySet()) {
			TopicPartition tp = entry.getKey();
			OffsetAndMetadata om = entry.getValue();
			log.debug("Flushing up to topic {}, partition {} and offset {}", tp.topic(), tp.partition(), om.offset());
		}

		if(sConfig.getString(SolaceSinkConstants.SOl_QUEUE) != null ) {
			boolean commited = sender.commit();
			if(!commited) {
				log.info("==============error in commiting transaction, shutting down");
				stop();
			}
		}

	}



}
