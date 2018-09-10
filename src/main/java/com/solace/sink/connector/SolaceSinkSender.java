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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.ProducerFlowProperties;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageProducer;
import com.solacesystems.jcsmp.transaction.TransactedSession;

public class SolaceSinkSender {
	private static final Logger log = LoggerFactory.getLogger(SolaceSinkSender.class);

	private SolaceSinkConfig sConfig;
	private XMLMessageProducer producer;
	private XMLMessageProducer TxProducer;
	private BytesXMLMessage message;
	private List<Topic> Topics = new ArrayList<Topic>();
	private Queue solQueue;
	private boolean useTxforQueue = false;
	private Class<?> cProcessor;
	private SolRecordProcessor processor;
	private String kafkaKey;
	private TransactedSession TxSession;
	private SolaceSinkTask sinkTask;
	private AtomicInteger msgCounter = new AtomicInteger();
	private Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<TopicPartition, OffsetAndMetadata>();

	public SolaceSinkSender(SolaceSinkConfig sConfig, JCSMPSession session, TransactedSession TxSession, SolaceSinkTask sinkTask) {
		this.sConfig = sConfig;
		this.TxSession = TxSession;
		this.sinkTask = sinkTask;

		kafkaKey = this.sConfig.getString(SolaceSinkConstants.SOL_KAFKA_MESSAGE_KEY);
		
		if(sConfig.getString(SolaceSinkConstants.SOl_QUEUE) != null) 
			solQueue = JCSMPFactory.onlyInstance().createQueue(sConfig.getString(SolaceSinkConstants.SOl_QUEUE));

		ProducerFlowProperties flowProps = new ProducerFlowProperties();
		flowProps.setAckEventMode(sConfig.getString(SolaceSinkConstants.SOL_ACK_EVENT_MODE));
		flowProps.setWindowSize(sConfig.getInt(SolaceSinkConstants.SOL_PUBLISHER_WINDOW_SIZE));



		try {
			producer = session.getMessageProducer(new SolStreamingMessageCallbackHandler());
			if(sConfig.getString(SolaceSinkConstants.SOl_QUEUE) != null) {
				TxProducer = TxSession.createProducer(flowProps, new SolStreamingMessageCallbackHandler(), new SolProducerEventCallbackHandler());
				log.info("=================TxSession status: {}", TxSession.getStatus().toString());
			}

		} catch (JCSMPException e) {
			log.info("Received Solace exception {}, with the following: {} ", e.getCause(), e.getStackTrace());
		}


		cProcessor = (this.sConfig.getClass(SolaceSinkConstants.SOL_RECORD_PROCESSOR));
		try {
			processor = (SolRecordProcessor) cProcessor.newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			log.info("=================Received exception while creating record processing class {}, with the following: {} ", e.getCause(), e.getStackTrace());
		}
	}

	public void createTopics() {
		String solaceTopics = sConfig.getString(SolaceSinkConstants.SOL_TOPICS);
		String sTopics[] = solaceTopics.split(",");
		int counter = 0;

		while(sTopics.length > counter) {
			Topics.add(JCSMPFactory.onlyInstance().createTopic(sTopics[counter].trim()));
			counter++;
		}
	}

	public void useTx(boolean Tx) {
		this.useTxforQueue = Tx;
	}

	public void sendRecord(SinkRecord record) {
		message = processor.processRecord(kafkaKey, record);
		offsets.put(new TopicPartition(record.topic(), record.kafkaPartition()), new OffsetAndMetadata(record.kafkaOffset()));
		log.trace("=================record details, topic: {}, Partition: {}, Offset: {}", record.topic(), record.kafkaPartition(), record.kafkaOffset());


		if(message.getAttachmentContentLength() == 0 || message.getAttachmentByteBuffer() == null) {
			log.info("==============Received record that had no data....discarded");
			return;
		}

		if(message.getUserData() == null) {
			log.trace("============Receive a Kafka record with no data ... discarded");
			return;
		} 

		if(useTxforQueue) {
			try {
				message.setDeliveryMode(DeliveryMode.PERSISTENT);
				TxProducer.send(message , solQueue);
				msgCounter.getAndIncrement();
				log.trace("===============Count of TX message is now: {}", msgCounter.get());
			} catch (JCSMPException e) {
				log.info("=================Received exception while sending message to queue {}:  {}, with the following: {} ", solQueue.getName(), e.getCause(), e.getStackTrace());
			} 

		} 

		if( Topics.size() != 0 && message.getDestination() == null ) {
			message.setDeliveryMode(DeliveryMode.DIRECT);
			int count = 0;
			while(Topics.size() > count) {
				try {
					producer.send(message, Topics.get(count));
					count++;
				} catch (JCSMPException e) {
					log.trace("=================Received exception while sending message to topic {}:  {}, with the following: {} ", Topics.get(count).getName(), e.getCause(), e.getStackTrace());

				}
				count++;
			}

		}
		// Solace limits transaction size to 255 messages so need to force commit
		if(useTxforQueue && msgCounter.get() > 200) {
			log.debug("================Manually Flushing Offsets");
			sinkTask.flush(offsets);
		}

	}

	public synchronized boolean commit() {
		boolean commited = true;
		try {
			if (useTxforQueue) {
				TxSession.commit();
				commited = true;
				msgCounter.set(0);
				log.debug("Comitted Solace records for transaction with status: {}", TxSession.getStatus().name());
			}
		} catch (JCSMPException e) {
			log.info("Received Solace exception {}, with the following: {} ", e.getCause(), e.getStackTrace());
			commited = false;
		}
		return commited;
	}

	public boolean shutdown() {
		boolean success = true;
		if(TxProducer != null)
			TxProducer.close();
		if(producer !=null)
			producer.close();

		return success;
	}

}
