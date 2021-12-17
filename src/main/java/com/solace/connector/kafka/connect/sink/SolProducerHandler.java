package com.solace.connector.kafka.connect.sink;

import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.ProducerFlowProperties;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.XMLMessage;
import com.solacesystems.jcsmp.XMLMessageProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SolProducerHandler implements AutoCloseable {
	private final SolaceSinkConnectorConfig config;
	private final SolSessionHandler sessionHandler;
	final XMLMessageProducer producer;
	private final AtomicInteger txMsgCount = new AtomicInteger();
	private final Runnable txAutoFlushCallback;
	private final ReadWriteLock lock = new ReentrantReadWriteLock();
	volatile XMLMessageProducer transactedProducer;
	volatile XMLMessageProducer topicProducer;
	volatile XMLMessageProducer queueProducer;

	private static final Logger log = LoggerFactory.getLogger(SolProducerHandler.class);

	public SolProducerHandler(final SolaceSinkConnectorConfig config,
							  final SolSessionHandler sessionHandler,
							  final Runnable txAutoFlushCallback) throws JCSMPException {
		this.config = config;
		this.sessionHandler = sessionHandler;
		this.txAutoFlushCallback = txAutoFlushCallback;
		this.producer = sessionHandler.getSession().getMessageProducer(new SolStreamingMessageCallbackHandler());

		if (config.getString(SolaceSinkConstants.SOl_QUEUE) != null) {
			// pre-init producer if queues are statically defined
			initQueueProducer();
		}

		if (config.getTopics().length > 0) {
			// pre-init producer if topics are statically defined
			initTopicProducer();
		}
	}

	public void send(final XMLMessage message, final Destination destination) throws JCSMPException {
		if (destination instanceof Queue) {
			if (queueProducer == null) {
				initQueueProducer();
			}
		} else {
			if (topicProducer == null) {
				initTopicProducer();
			}
		}

		Lock readLock = this.lock.readLock();
		readLock.lock();
		try {
			if (destination instanceof Queue) {
				message.setDeliveryMode(DeliveryMode.PERSISTENT);
				queueProducer.send(message, destination);
				if (config.getBoolean(SolaceSinkConstants.SOl_USE_TRANSACTIONS_FOR_QUEUE)) {
					autoFlush();
				}
			} else {
				if (config.getBoolean(SolaceSinkConstants.SOl_USE_TRANSACTIONS_FOR_TOPICS)) {
					message.setDeliveryMode(DeliveryMode.PERSISTENT);
					topicProducer.send(message, destination);
					autoFlush();
				} else {
					message.setDeliveryMode(DeliveryMode.DIRECT);
					topicProducer.send(message, destination);
				}
			}
		} finally {
			readLock.unlock();
		}
	}

	public AtomicInteger getTxMsgCount() {
		return txMsgCount;
	}

	private void autoFlush() {
		int txMsgCnt = txMsgCount.incrementAndGet();
		log.trace("================ Count of TX message is now: {}", txMsgCnt);
		if (txMsgCnt > config.getInt(SolaceSinkConstants.SOL_AUTOFLUSH_SIZE)-1) {
			txAutoFlushCallback.run();
		}
	}

	private void initTopicProducer() throws JCSMPException {
		Lock writeLock = this.lock.writeLock();
		writeLock.lock();
		try {
			if (topicProducer == null) {
				if (config.getBoolean(SolaceSinkConstants.SOl_USE_TRANSACTIONS_FOR_TOPICS)) {
					this.topicProducer = createTransactedProducer();
				} else {
					this.topicProducer = producer;
				}
			}
		} finally {
			writeLock.unlock();
		}
	}

	private void initQueueProducer() throws JCSMPException {
		Lock writeLock = this.lock.writeLock();
		writeLock.lock();
		try {
			if (queueProducer == null) {
				if (config.getBoolean(SolaceSinkConstants.SOl_USE_TRANSACTIONS_FOR_QUEUE)) {
					// Using transacted session for queue
					queueProducer = createTransactedProducer();
				} else {
					// Not using transacted session for queue
					queueProducer = sessionHandler.getSession().createProducer(createProducerFlowProperties(),
							new SolStreamingMessageCallbackHandler(), new SolProducerEventCallbackHandler());
				}
			}
		} finally {
			writeLock.unlock();
		}
	}

	private XMLMessageProducer createTransactedProducer() throws JCSMPException {
		if (transactedProducer == null) {
			Lock writeLock = this.lock.writeLock();
			writeLock.lock();
			try {
				if (transactedProducer == null) {
					sessionHandler.createTxSession();
					transactedProducer = sessionHandler.getTxSession().createProducer(createProducerFlowProperties(),
							new SolStreamingMessageCallbackHandler(), new SolProducerEventCallbackHandler());
					log.info("================ txSession status: {}",
							sessionHandler.getTxSession().getStatus().toString());
				}
			} finally {
				writeLock.unlock();
			}
		}
		return transactedProducer;
	}

	private ProducerFlowProperties createProducerFlowProperties() {
		ProducerFlowProperties flowProps = new ProducerFlowProperties();
		flowProps.setAckEventMode(config.getString(SolaceSinkConstants.SOL_ACK_EVENT_MODE));
		flowProps.setWindowSize(config.getInt(SolaceSinkConstants.SOL_PUBLISHER_WINDOW_SIZE));
		return flowProps;
	}

	@Override
	public void close() {
		Lock writeLock = lock.writeLock();
		writeLock.lock();
		try {
			if (queueProducer != null && !queueProducer.isClosed()) {
				queueProducer.close();
			}

			if (topicProducer != null && !topicProducer.isClosed()) {
				topicProducer.close();
			}

			if (transactedProducer != null && !transactedProducer.isClosed()) {
				transactedProducer.close();
			}

			if (producer != null && !producer.isClosed()) {
				producer.close();
			}
		} finally {
			writeLock.unlock();
		}
	}
}
